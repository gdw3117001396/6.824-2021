package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const excuteTimeOut = 500 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType      string
	Key         string
	Value       string
	ClientId    int64
	SequenceNum int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	// Your definitions here.
	kvStore               map[string]string
	indexChanMap          map[int]chan CommandResponse
	clientLastSequenceNum map[int64]int64
	lastApplied           int
}

type CommandResponse struct {
	Err   Err
	Value string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{
		OpType:      "Get",
		Key:         args.Key,
		Value:       "",
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	reply.Err, reply.Value = kv.CommandHandler(cmd)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op{
		OpType:      args.Op,
		Key:         args.Key,
		Value:       args.Value,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	reply.Err, _ = kv.CommandHandler(cmd)
}

func (kv *KVServer) CommandHandler(cmd Op) (Err, string) {
	kv.mu.Lock()
	if cmd.OpType != "Get" && kv.isduplicateSequenceNum(cmd.ClientId, cmd.SequenceNum) {
		kv.mu.Unlock()
		return OK, ""
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return ErrWrongLeader, ""
	}
	// DPrintf("kvserver %d 写入了命令", kv.me)
	kv.mu.Lock()
	ch := kv.getIndexChanL(index)
	kv.mu.Unlock()

	// 每个session都删除即可
	defer kv.deleteIndexChan(index)
	select {
	case response := <-ch:
		DPrintf("返回Err:%v, value%v", response.Err, response.Value)
		return response.Err, response.Value
	case <-time.After(excuteTimeOut):
		// 超时就返回错误，重新发送
		return ErrWrongLeader, ""
	}
}

func (kv *KVServer) getIndexChanL(index int) chan CommandResponse {
	if _, ok := kv.indexChanMap[index]; !ok {
		kv.indexChanMap[index] = make(chan CommandResponse, 1)
	}
	return kv.indexChanMap[index]
}

func (kv *KVServer) deleteIndexChan(index int) {
	kv.mu.Lock()
	close(kv.indexChanMap[index])
	delete(kv.indexChanMap, index)
	kv.mu.Unlock()
}

// 命令是否重复
func (kv *KVServer) isduplicateSequenceNum(clientId int64, sequenceNum int64) bool {
	seqId, ok := kv.clientLastSequenceNum[clientId]
	return ok && sequenceNum <= seqId
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		m := <-kv.applyCh
		// DPrintf("尝试提交信息message %v", m)
		if m.SnapshotValid {
			// 这里参数不要传成Command的了，有点尴尬找了很久的错误才发现这个问题
			if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
				kv.mu.Lock()
				DPrintf("kvserver %d 安装了从raft服务传来的快照", kv.me)
				kv.InstallSnapshotL(m.Snapshot)
				kv.lastApplied = m.CommandIndex
				kv.mu.Unlock()
			}
		} else if m.CommandValid {
			kv.mu.Lock()
			if m.CommandIndex <= kv.lastApplied {
				DPrintf("丢弃旧信息")
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = m.CommandIndex
			op := m.Command.(Op)
			response := CommandResponse{}
			if op.OpType != "Get" && kv.isduplicateSequenceNum(op.ClientId, op.SequenceNum) {
				DPrintf("不提交重复信息 %v 因为 client的 %v 最新的seqId 是 %v", m, op.ClientId, kv.clientLastSequenceNum[op.ClientId])
				response.Err, response.Value = OK, ""
			} else {
				kv.clientLastSequenceNum[op.ClientId] = op.SequenceNum
				switch op.OpType {
				case "Get":
					value, ok := kv.kvStore[op.Key]
					if ok {
						response.Err, response.Value = OK, value
					} else {
						response.Err, response.Value = ErrNoKey, ""
					}
					// DPrintf("Server Get, key: %v, value: %v", op.Key, op.Value)
				case "Put":
					kv.kvStore[op.Key] = op.Value
					response.Err, response.Value = OK, ""
					// DPrintf("Server Put, key: %v, value: %v", op.Key, op.Value)
				case "Append":
					kv.kvStore[op.Key] += op.Value
					response.Err, response.Value = OK, ""
					// DPrintf("Server Append, key: %v, value: %v", op.Key, op.Value)
				}
			}
			currentTerm, isLeader := kv.rf.GetState()
			if isLeader && m.CommandTerm == currentTerm {
				DPrintf("发送消息回给客户端了 Command:%v Response:%v commitIndex:%v currentTerm: %v", op, response, m.CommandIndex, currentTerm)
				ch := kv.getIndexChanL(m.CommandIndex)
				kv.mu.Unlock()
				ch <- response
				kv.mu.Lock()
			}
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				DPrintf("kv server %d 主动安装了快照", kv.me)
				kv.SnapShotL(kv.lastApplied)
			}
			kv.mu.Unlock()
		}

	}
}

// 主动安装快照，调用raft 的Snapshot来安装快照
func (kv *KVServer) SnapShotL(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.clientLastSequenceNum)
	e.Encode(kv.lastApplied)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *KVServer) InstallSnapshotL(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvStore map[string]string
	var clientLastSequenceNum map[int64]int64
	var lastApplied int
	if d.Decode(&kvStore) != nil ||
		d.Decode(&clientLastSequenceNum) != nil ||
		d.Decode(&lastApplied) != nil {
		DPrintf("安装快照失败")
		// os.Exit(1)
	} else {
		kv.kvStore = kvStore
		kv.clientLastSequenceNum = clientLastSequenceNum
		kv.lastApplied = lastApplied
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.indexChanMap = make(map[int]chan CommandResponse)
	kv.kvStore = make(map[string]string)
	kv.clientLastSequenceNum = make(map[int64]int64)
	kv.lastApplied = 0
	kv.InstallSnapshotL(persister.ReadSnapshot())
	// You may need initialization code here.
	go kv.applier()
	return kv
}

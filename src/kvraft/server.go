package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key    string
	Value  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	kvStore      map[string]string
	indexChanMap map[int]chan CommandResponse
	lastApplied  int
}

type CommandResponse struct {
	Err   Err
	Value string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{
		OpType: "Get",
		Key:    args.Key,
		Value:  "",
	}
	reply.Err, reply.Value = kv.CommandHandler(cmd)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op{
		OpType: args.Op,
		Key:    args.Key,
		Value:  args.Value,
	}
	reply.Err, _ = kv.CommandHandler(cmd)
}

func (kv *KVServer) CommandHandler(cmd Op) (Err, string) {
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return ErrWrongLeader, ""
	}
	kv.mu.Lock()
	ch := kv.getIndexChanL(index)
	kv.mu.Unlock()
	response := <-ch
	// 每个session都删除即可
	go kv.deleteIndexChan(index)
	return response.Err, response.Value
}

func (kv *KVServer) getIndexChanL(index int) chan CommandResponse {
	if _, ok := kv.indexChanMap[index]; !ok {
		kv.indexChanMap[index] = make(chan CommandResponse, 1)
	}
	return kv.indexChanMap[index]
}

func (kv *KVServer) deleteIndexChan(index int) {
	kv.mu.Lock()
	delete(kv.indexChanMap, index)
	kv.mu.Unlock()
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
		for m := range kv.applyCh {
			if m.CommandValid {
				kv.mu.Lock()
				op := m.Command.(Op)
				if kv.lastApplied >= m.CommandIndex {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = m.CommandIndex
				response := CommandResponse{}
				switch op.OpType {
				case "Get":
					value, ok := kv.kvStore[op.Key]
					if ok {
						response.Err, response.Value = OK, value
					} else {
						response.Err, response.Value = ErrNoKey, ""
					}
				case "Put":
					kv.kvStore[op.Key] = op.Value
					response.Err, response.Value = OK, ""
				case "Append":
					kv.kvStore[op.Key] += op.Value
					response.Err, response.Value = OK, ""
				}

				currentTerm, isLeader := kv.rf.GetState()
				if isLeader {
					DPrintf("发送消息回给客户端了 Command:%v Response:%v commitIndex:%v currentTerm: %v", op, response, m.CommandIndex, currentTerm)
					ch := kv.getIndexChanL(m.CommandIndex)
					kv.mu.Unlock()
					ch <- response
					kv.mu.Lock()
				}
				kv.mu.Unlock()
			}
		}
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.indexChanMap = make(map[int]chan CommandResponse)
	kv.kvStore = make(map[string]string)
	kv.lastApplied = 0
	// You may need initialization code here.
	go kv.applier()
	return kv
}

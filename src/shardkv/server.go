package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type ShardKV struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	make_end  func(string) *labrpc.ClientEnd
	gid       int
	ctrlers   []*labrpc.ClientEnd
	dead      int32
	mck       *shardctrler.Clerk
	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big
	lastApplied  int

	// Your definitions here.
	lastConfig    shardctrler.Config // 上一个config(用于获取新增分片从哪个集群拉取内容)
	currentConfig shardctrler.Config // 当前的config

	kvStore        map[int]*Shard             // shardId->Shard
	lastOperations map[int64]OperationContext // commandId->OperationContext防止重复
	indexChanMap   map[int]chan *CommandResponse
}

// 提交给raft进行日志同步
func (kv *ShardKV) Execute(command Command, response *CommandResponse) {
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getIndexChanL(index)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		response.Value, response.Err = result.Value, result.Err
		DPrintf("server %d group %d 返回Err:%v, value%v", kv.me, kv.gid, response.Err, response.Value)
	case <-time.After(ExecuteTimeout):
		response.Err = ErrTimeout
	}
	go kv.deleteIndexChan(index)
}

func (kv *ShardKV) canServeL(shardID int) bool {
	return kv.currentConfig.Shards[shardID] == kv.gid && (kv.kvStore[shardID].Status == Serving || kv.kvStore[shardID].Status == GCing)
}

func (kv *ShardKV) getIndexChanL(index int) chan *CommandResponse {
	if _, ok := kv.indexChanMap[index]; !ok {
		kv.indexChanMap[index] = make(chan *CommandResponse, 1)
	}
	return kv.indexChanMap[index]
}

func (kv *ShardKV) deleteIndexChan(index int) {
	kv.mu.Lock()
	close(kv.indexChanMap[index])
	delete(kv.indexChanMap, index)
	kv.mu.Unlock()
}

// 命令是否重复
func (kv *ShardKV) isduplicateSequenceNumL(clientId int64, sequenceNum int64) bool {
	OperationContext, ok := kv.lastOperations[clientId]
	return ok && sequenceNum <= OperationContext.MaxAppliedCommandId
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 所有涉及修改集群分片状态的操作都应该通过 raft 日志的方式去提交，这样才可以保证同一 raft 组内的所有分片数据和状态一致。
func (kv *ShardKV) applier() {
	for !kv.killed() {
		for m := range kv.applyCh {
			DPrintf("shardkv server %d group : %d 尝试提交信息m.Command %v", kv.me, kv.gid, m.Command)
			if m.CommandValid {
				kv.mu.Lock()
				if m.CommandIndex <= kv.lastApplied {
					DPrintf("shardkv server %d group : %d 丢弃旧信息", kv.me, kv.gid)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = m.CommandIndex
				var response *CommandResponse
				command := m.Command.(Command)
				switch command.Op {
				case Execute:
					operation := command.Data.(Op)
					response = kv.applyExecuteL(&operation)
				case PullConfig:
					nextConfig := command.Data.(shardctrler.Config)
					response = kv.applyPullConfigL(&nextConfig)
				case PullShards:
					shardsInfo := command.Data.(ShardOperationReply)
					response = kv.applyPullShardsL(&shardsInfo)
				case PushGC:
					shardsInfo := command.Data.(ShardOperationArgs)
					response = kv.applyPushGCL(&shardsInfo)
				case CheckLeaderEmptyEntry:
					response = kv.applyCheckLeaderEmptyEntryL()
				}

				currentTerm, isLeader := kv.rf.GetState()
				if isLeader && m.CommandTerm == currentTerm {
					DPrintf("发送消息回给客户端了 Command:%v Response:%v commitIndex:%v currentTerm: %v", command, response, m.CommandIndex, currentTerm)
					ch := kv.getIndexChanL(m.CommandIndex)
					ch <- response
				}

				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
					DPrintf("kv server %d 主动安装了快照", kv.me)
					kv.SnapShotL(kv.lastApplied)
				}
				kv.mu.Unlock()
			} else if m.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
					DPrintf("kvserver %d 安装了从raft服务传来的快照", kv.me)
					kv.InstallSnapshotL(m.Snapshot)
					kv.lastApplied = m.CommandIndex
				}
				kv.mu.Unlock()
			}
		}
	}
}

// 空日志检查, 防止活锁
func (kv *ShardKV) CheckLeaderEmptyEntry() {
	if !kv.rf.HasLogInCurrentTerm() {
		kv.Execute(NewcheckLeaderEmptyEntryCommand(), &CommandResponse{})
	}
}

func (kv *ShardKV) applyCheckLeaderEmptyEntryL() *CommandResponse {
	return &CommandResponse{OK, ""}
}

// 主动安装快照，调用raft 的Snapshot来安装快照
func (kv *ShardKV) SnapShotL(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.lastOperations)
	e.Encode(kv.currentConfig)
	e.Encode(kv.lastConfig)
	e.Encode(kv.lastApplied)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *ShardKV) InstallSnapshotL(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvStore map[int]*Shard
	var lastOperations map[int64]OperationContext
	var currentConfig shardctrler.Config
	var lastConfig shardctrler.Config
	var lastApplied int
	if d.Decode(&kvStore) != nil ||
		d.Decode(&lastOperations) != nil ||
		d.Decode(&currentConfig) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&lastApplied) != nil {
		DPrintf("安装快照失败")
		// os.Exit(1)
	} else {
		kv.kvStore = kvStore
		kv.lastOperations = lastOperations
		kv.currentConfig = currentConfig
		kv.lastConfig = lastConfig
		kv.lastApplied = lastApplied
	}
}

func (kv *ShardKV) initKvStore() {
	for i := 0; i < shardctrler.NShards; i++ {
		if _, ok := kv.kvStore[i]; !ok {
			kv.kvStore[i] = NewShard()
		}
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Command{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardOperationArgs{})
	labgob.Register(ShardOperationReply{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.kvStore = make(map[int]*Shard)
	kv.initKvStore()
	kv.lastOperations = make(map[int64]OperationContext)
	kv.indexChanMap = make(map[int]chan *CommandResponse)
	kv.currentConfig = shardctrler.Config{Groups: make(map[int][]string)}
	kv.lastConfig = shardctrler.Config{Groups: make(map[int][]string)}
	kv.lastApplied = 0
	kv.InstallSnapshotL(persister.ReadSnapshot())

	go kv.applier()
	// 拉取配置
	go kv.deamon(kv.PullConfig, PullConfigTimeout)

	// 拉取数据
	go kv.deamon(kv.PullShards, PullShardsTimeout)

	// 垃圾删除
	go kv.deamon(kv.PushGC, PushGCTimeout)

	// 空日志检测
	go kv.deamon(kv.CheckLeaderEmptyEntry, CheckLeaderEmptyEntryTimeout)
	return kv
}

func (kv *ShardKV) deamon(action func(), timeout time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}

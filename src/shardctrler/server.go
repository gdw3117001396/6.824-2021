package shardctrler

import (
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

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

const excuteTimeOut = 500 * time.Millisecond

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32
	// Your data here.

	configs               []Config // indexed by config num
	indexChanMap          map[int]chan CommandResponse
	clientLastSequenceNum map[int64]int64
}

type Op struct {
	// Your data here.
	Servers     map[int][]string // new GID -> servers mappings(join)
	GIDs        []int            // leave
	Shard       int              // move
	GID         int              // move
	Num         int              // query
	Optype      string
	ClientId    int64
	SequenceNum int64
}

type CommandResponse struct {
	WrongLeader bool
	Config      Config
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	cmd := Op{
		Optype:      OpJoin,
		Servers:     args.Servers,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	reply.WrongLeader, _ = sc.CommandHandler(cmd)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	cmd := Op{
		Optype:      OpLeave,
		GIDs:        args.GIDs,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	reply.WrongLeader, _ = sc.CommandHandler(cmd)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	cmd := Op{
		Optype:      OpMove,
		Shard:       args.Shard,
		GID:         args.GID,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}
	reply.WrongLeader, _ = sc.CommandHandler(cmd)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	if args.Num >= 0 && args.Num < len(sc.configs) {
		reply.WrongLeader = false
		reply.Config = sc.configs[args.Num]
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	cmd := Op{
		Optype:      OpQuery,
		Num:         args.Num,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	reply.WrongLeader, reply.Config = sc.CommandHandler(cmd)
}

func (sc *ShardCtrler) CommandHandler(cmd Op) (bool, Config) {
	sc.mu.Lock()
	if cmd.Optype != OpQuery && sc.isduplicateSequenceNumL(cmd.ClientId, cmd.SequenceNum) {
		sc.mu.Unlock()
		return false, Config{}
	}
	sc.mu.Unlock()
	index, _, isLeader := sc.rf.Start(cmd)

	if !isLeader {
		return true, Config{}
	}
	// DPrintf("start")
	sc.mu.Lock()
	ch := sc.getIndexChanL(index)
	sc.mu.Unlock()
	defer sc.deleteIndexChanL(index)
	select {
	case response := <-ch:
		DPrintf("收到了回复, Config: %v", response.Config)
		return response.WrongLeader, response.Config
	case <-time.After(excuteTimeOut):
		DPrintf("超时")
		return true, Config{}
	}
}

func (sc *ShardCtrler) getIndexChanL(index int) chan CommandResponse {
	if _, ok := sc.indexChanMap[index]; !ok {
		sc.indexChanMap[index] = make(chan CommandResponse, 1)
	}
	return sc.indexChanMap[index]
}

func (sc *ShardCtrler) deleteIndexChanL(index int) {
	sc.mu.Lock()
	close(sc.indexChanMap[index])
	delete(sc.indexChanMap, index)
	sc.mu.Unlock()
}

// 命令是否重复
func (sc *ShardCtrler) isduplicateSequenceNumL(clientId int64, sequenceNum int64) bool {
	seqId, ok := sc.clientLastSequenceNum[clientId]
	return ok && sequenceNum <= seqId
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}
func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		lastApplied := 0
		for m := range sc.applyCh {
			DPrintf("server %d ,开始提交信息, m.Command %v", sc.me, m.Command)
			if m.CommandValid && m.CommandIndex > lastApplied {
				sc.mu.Lock()
				op := m.Command.(Op)
				lastApplied = m.CommandIndex
				response := CommandResponse{}
				if op.Optype != OpQuery && sc.isduplicateSequenceNumL(op.ClientId, op.SequenceNum) {
					DPrintf("不提交重复信息 %v 因为 client的 %v 最新的seqId 是 %v", m, op.ClientId, sc.clientLastSequenceNum[op.ClientId])
					response.WrongLeader, response.Config = false, Config{}
				} else {
					sc.clientLastSequenceNum[op.ClientId] = op.SequenceNum
					if op.Optype == OpQuery {
						if op.Num >= 0 && op.Num < len(sc.configs) {
							response.WrongLeader, response.Config = false, sc.configs[op.Num]
						} else {
							response.WrongLeader, response.Config = false, sc.configs[len(sc.configs)-1]
						}
					} else {
						sc.updateConfigL(op)
						response.WrongLeader, response.Config = false, Config{}
					}
				}
				currentTerm, isLeader := sc.rf.GetState()
				if isLeader && m.CommandTerm == currentTerm {
					DPrintf("发送消息回给客户端了 Command:%v Response:%v commitIndex:%v currentTerm: %v", op, response, m.CommandIndex, currentTerm)
					ch := sc.getIndexChanL(m.CommandIndex)
					sc.mu.Unlock()
					ch <- response
				} else {
					sc.mu.Unlock()
				}
			}
		}
	}
}

func (sc *ShardCtrler) updateConfigL(op Op) Config {
	nextCfg := sc.getConfigL()
	opType := op.Optype
	DPrintf("ServerNum: %d, opType: %s", sc.me, opType)
	switch opType {
	case OpJoin:
		sc.opJoinL(&nextCfg, op.Servers)
	case OpLeave:
		sc.opLeaveL(&nextCfg, op.GIDs)
	case OpMove:
		sc.opMoveL(&nextCfg, op.Shard, op.GID)
	}
	DPrintf("更新了分片控制器: %v: Op: %v nextCfg: %v lastCfg:%v", sc.me, opType, nextCfg, sc.configs[len(sc.configs)-1])
	sc.configs = append(sc.configs, nextCfg)
	return nextCfg
}
func (sc *ShardCtrler) opJoinL(config *Config, joinServers map[int][]string) {
	for gid, servers := range joinServers {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		config.Groups[gid] = newServers
	}
	if len(config.Groups) > 0 {
		sc.balanceL(config)
	}
}

func (sc *ShardCtrler) opLeaveL(config *Config, GIDs []int) {
	for _, gid := range GIDs {
		delete(config.Groups, gid)
	}
	if len(config.Groups) > 0 {
		sc.balanceL(config)
	}
}

func (sc *ShardCtrler) opMoveL(config *Config, shard int, GID int) {
	if _, ok := config.Groups[GID]; ok {
		config.Shards[shard] = GID
	}
}

func (sc *ShardCtrler) balanceL(config *Config) {
	bufferShards := []int{}       // 有哪些分片Shard分配不均匀的，放入buffer等待分配
	gidBuckets := map[int][]int{} // 统计目前每个gid集群服务哪些Shard
	newServerNum := len(config.Groups)
	avgNum := NShards / newServerNum
	for gid := range config.Groups {
		gidBuckets[gid] = []int{}
	}
	// 看看哪些分配需要移动
	for shard, gid := range config.Shards {
		if shardList, ok := gidBuckets[gid]; !ok {
			// 若上一个配置的分片Shard的gid不在新配置的Groups中，
			// 说明该分片要重新分配，把分片放入bufferShards
			bufferShards = append(bufferShards, shard)
		} else {
			if len(shardList) >= avgNum {
				bufferShards = append(bufferShards, shard)
			} else {
				gidBuckets[gid] = append(gidBuckets[gid], shard)
			}
		}
	}
	// 排序，保证每个raft节点都是相同的顺序更改配置
	var keys []int
	for k, _ := range gidBuckets {
		keys = append(keys, k)
	}
	// DPrintf("sort before %v, gidBuckets %v", keys, gidBuckets)
	sort.Ints(keys)
	// DPrintf("sort after %v", keys)

	bufferShardsIndex := 0
	for _, k := range keys {
		shardList := gidBuckets[k]
		// 新配中的gid服务分片少于平均值的，加入buffer中的分片
		if len(shardList) < avgNum {
			for i := 0; i < avgNum-len(shardList) && bufferShardsIndex < len(bufferShards); i++ {
				config.Shards[bufferShards[bufferShardsIndex]] = k
				bufferShardsIndex += 1
			}
		}
	}

	index := 0
	for i := bufferShardsIndex; i < len(bufferShards); i++ {
		// buffer还有剩余分片，并且该分片原gid不在新配置汇总，则分给其他新gid
		if _, ok := config.Groups[config.Shards[bufferShards[i]]]; !ok {
			config.Shards[bufferShards[i]] = index
			index++
			DPrintf("buffer还有剩余的分片,分给了%v, 总共有 %d groups", keys[index], len(gidBuckets))
		}
	}
}
func (sc *ShardCtrler) getConfigL() Config {
	lastCfg := sc.configs[len(sc.configs)-1]
	nextCfg := Config{
		Num:    lastCfg.Num + 1,
		Shards: lastCfg.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range lastCfg.Groups {
		nextCfg.Groups[gid] = append(nextCfg.Groups[gid], servers...)
	}
	return nextCfg
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientLastSequenceNum = make(map[int64]int64)
	sc.indexChanMap = make(map[int]chan CommandResponse)
	// DPrintf("开启服务器")
	go sc.applier()
	return sc
}

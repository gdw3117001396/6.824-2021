package shardkv

import (
	"log"
	"time"

	"6.824/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//
type Err string

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOutDated    = "ErrOutDated"
	ErrTimeout     = "ErrTimeout"
	ErrNotReady    = "ErrNotReady"
)

const (
	ExecuteTimeout               = 500 * time.Millisecond
	PullConfigTimeout            = 100 * time.Millisecond
	PullShardsTimeout            = 50 * time.Millisecond
	PushGCTimeout                = 50 * time.Millisecond
	CheckLeaderEmptyEntryTimeout = 200 * time.Millisecond
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardStatus int

// Serving：分片的默认状态，
// 如果当前 raft 组在当前 config 下负责管理此分片，则该分片可以提供读写服务，否则该分片暂不可以提供读写服务，但不会阻塞配置更新协程拉取新配置。

// Pulling：表示当前 raft 组在当前 config 下负责管理此分片，暂不可以提供读写服务，
// 需要当前 raft 组从上一个配置该分片所属 raft 组拉数据过来之后才可以提供读写服务，系统会有一个分片迁移协程检测所有分片的 Pulling 状态，
// 接着以 raft 组为单位去对应 raft 组拉取数据，接着尝试重放该分片的所有数据到本地并将分片状态置为 Serving，以继续提供服务。

// BePulling:表示当前 raft 组在当前 config 下不负责管理此分片，不可以提供读写服务，但当前 raft 组在上一个 config 时复制管理此分片，
// 因此当前 config 下负责管理此分片的 raft 组拉取完数据后会向本 raft 组发送分片清理的 rpc，接着本 raft 组将数据清空并重置为 serving 状态即可。

// GCing: 表示当前 raft 组在当前 config 下负责管理此分片，可以提供读写服务，但需要清理掉上一个配置该分片所属 raft 组的数据。
// 系统会有一个分片清理协程检测所有分片的 GCing 状态，接着以 raft 组为单位去对应 raft 组删除数据，
// 一旦远程 raft 组删除数据成功，则本地会尝试将相关分片的状态置为 Serving。
const (
	Serving ShardStatus = iota
	Pulling
	BePulling
	GCing
)

type OperationContext struct {
	MaxAppliedCommandId int64
	LastResponse        *CommandResponse
}

func (context OperationContext) deepCopy() OperationContext {
	return OperationContext{
		MaxAppliedCommandId: context.MaxAppliedCommandId,
		LastResponse:        &CommandResponse{context.LastResponse.Err, context.LastResponse.Value},
	}
}

type OperationOp string

const (
	OpPut    OperationOp = "Put"
	OpAppend OperationOp = "Append"
	OpGet    OperationOp = "Get"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	CommandId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	CommandId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        OperationOp
	ClientId  int64
	CommandId int64
}

// Command
type Command struct {
	Op   CommandType
	Data interface{}
}

func NewExecuteCommand(op *Op) Command {
	return Command{Execute, *op}
}

func NewPullConfigCommand(config *shardctrler.Config) Command {
	return Command{PullConfig, *config}
}

func NewPullShardsCommand(response *ShardOperationReply) Command {
	return Command{PullShards, *response}
}

func NewPushGCCommand(request *ShardOperationArgs) Command {
	return Command{PushGC, *request}
}

func NewcheckLeaderEmptyEntryCommand() Command {
	return Command{CheckLeaderEmptyEntry, nil}
}

type CommandType string

const (
	Execute               CommandType = "Execute"
	PullConfig            CommandType = "PullConfig"
	PullShards            CommandType = "PullShards"
	PushGC                CommandType = "PushGC"
	CheckLeaderEmptyEntry CommandType = "checkLeaderEmptyEntry"
)

type CommandResponse struct {
	Err   Err
	Value string
}

// 拉取分片和通知可以删除分片了
type ShardOperationArgs struct {
	ConfigNum int
	ShardIDs  []int
}

type ShardOperationReply struct {
	Err            Err
	ConfigNum      int
	Shards         map[int]map[string]string
	LastOperations map[int64]OperationContext
}

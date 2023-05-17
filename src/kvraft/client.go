package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId    int
	clientId    int64 // 客户端id
	sequenceNum int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.sequenceNum = 0
	ck.leaderId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:         key,
		ClientId:    ck.clientId,
		SequenceNum: atomic.AddInt64(&ck.sequenceNum, 1),
	}
	for {
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			DPrintf("Client: %v 收到了成功的get回复 %v", ck.clientId, reply.Value)
			return reply.Value
		} else {
			// DPrintf("Client: %v 发送get超时或者kvServer[%d]不是leader", ck.clientId, ck.leaderId)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		// 稍等个一会再重试？
		time.Sleep(10 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		ClientId:    ck.clientId,
		SequenceNum: atomic.AddInt64(&ck.sequenceNum, 1),
	}
	// DPrintf("Clerk key %s, value %s", key, value)
	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			DPrintf("Client: %v 收到了成功的PutAppend回复", ck.clientId)
			break
		} else {
			// DPrintf("Client: %v 发送PutAppendt超时或者kvServer[ %d ]不是leader", ck.clientId, ck.leaderId)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		// 稍等个一会再重试
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

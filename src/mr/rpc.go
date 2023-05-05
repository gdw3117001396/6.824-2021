package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// 请求任务
type RequstArgs struct {
	WorkerId int
}

// 请求任务回复
type RequstReply struct {
	File      string
	TaskId    int
	TaskType  string
	AllFinish bool
	NMap      int
	NReduce   int
}

// 提交任务
type CommitArgs struct {
	WorkerId int
	TaskId   int
	TaskType string
}

// 回复任务
type CommitReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

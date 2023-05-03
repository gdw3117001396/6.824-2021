package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var mlog *log.Logger
var mlogFile *os.File

func mlogInit() {
	workerId := os.Getpid()
	logName := "coordinator" + strconv.Itoa(workerId) + ".log"
	mlogFile, _ = os.Create(logName)
	mlog = log.New(mlogFile, "", log.Lmicroseconds|log.Lshortfile)
}

type Coordinator struct {
	// Your definitions here.
	files       []string
	mapTasks    []int
	reduceTasks []int
	mapCount    int
	mapFinish   bool
	allFinish   bool
	mutex       sync.Mutex
}

const taskIdle = 0
const taskRunning = 1
const taskDown = 2

// 计时器
func (c *Coordinator) startTimer(taskType string, index int) {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	<-timer.C
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if taskType == "map" {
		if c.mapTasks[index] == taskRunning {
			c.mapTasks[index] = taskIdle
			mlog.Printf("%s task %d timeout!\n", taskType, index)
			mlog.Println("now map task", c.mapTasks)
		}
	} else if taskType == "reduce" {
		if c.reduceTasks[index] == taskRunning {
			c.reduceTasks[index] = taskIdle
			mlog.Printf("%s task %d timeout!\n", taskType, index)
			mlog.Println("now reduce task", c.reduceTasks)
		}
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Work(args *RequstArgs, reply *RequstReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.allFinish {
		reply.AllFinish = true
		return nil
	}
	for i, taskState := range c.mapTasks {
		if taskState != taskIdle {
			continue
		}
		reply.File = c.files[i]
		reply.TaskId = i
		reply.TaskType = "map"
		reply.AllFinish = false
		reply.NMap = len(c.mapTasks)
		reply.NReduce = len(c.reduceTasks)
		c.mapTasks[i] = taskRunning
		go c.startTimer("map", i)
		mlog.Println("map task", i, "has sent to worker ", args.WorkerId)
		mlog.Println("now map task", c.mapTasks)
		return nil
	}
	if !c.mapFinish {
		reply.AllFinish = false
		return nil
	}
	for i, taskState := range c.reduceTasks {
		if taskState != taskIdle {
			continue
		}
		reply.TaskId = i
		reply.TaskType = "reduce"
		reply.AllFinish = false
		reply.NMap = len(c.mapTasks)
		reply.NReduce = len(c.reduceTasks)
		c.reduceTasks[i] = taskRunning
		go c.startTimer("reduce", i)
		mlog.Println("reduce task", i, "has sent to worker", args.WorkerId)
		mlog.Println("now reduce task", c.reduceTasks)
		return nil
	}
	if c.allFinish {
		reply.AllFinish = true
	} else {
		reply.AllFinish = false
	}
	return nil
}

func (c *Coordinator) Commit(args *CommitArgs, reply *CommitReply) error {
	mlog.Println("worker", args.WorkerId, "commit", args.TaskType, "task", args.TaskId)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.TaskType == "map" {
		c.mapTasks[args.TaskId] = taskDown
		c.mapCount++
		if c.mapCount == len(c.files) {
			c.mapFinish = true
		}
	} else if args.TaskType == "reduce" {
		c.reduceTasks[args.TaskId] = taskDown
	}
	for _, state := range c.reduceTasks {
		if state != taskDown {
			return nil
		}
	}
	c.allFinish = true
	mlog.Println("all tasks finish")
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.allFinish {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.mapTasks = make([]int, len(files))
	c.reduceTasks = make([]int, nReduce)
	c.mapCount = 0
	c.mapFinish = false
	c.allFinish = false
	mlogInit()
	c.server()
	return &c
}

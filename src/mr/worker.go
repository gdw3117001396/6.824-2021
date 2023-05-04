package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//

var wlog *log.Logger
var wlogFile *os.File

func wlogInit() {
	workerId := os.Getpid()
	logName := "worker" + strconv.Itoa(workerId) + ".log"
	wlogFile, _ = os.Create(logName)
	wlog = log.New(wlogFile, "", log.Lmicroseconds|log.Lshortfile)
}

type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	wlogInit()              // 初始化日志
	workerId := os.Getpid() // 为日志服务
	for {
		args := RequstArgs{WorkerId: workerId}
		reply := RequstReply{}
		working := call("Coordinator.Work", &args, &reply) // 通过RPC请求任务
		if reply.AllFinish || !working {                   // 如果结束，或者coordinator关闭了，认为是任务完成了
			wlog.Println("finished")
			return
		}
		wlog.Println("task info", reply)
		if reply.TaskType == "map" {
			MapWork(reply, mapf) // 做map任务
			args2 := CommitArgs{WorkerId: workerId, TaskId: reply.TaskId, TaskType: "map"}
			reply2 := CommitReply{}
			working2 := call("Coordinator.Commit", &args2, &reply2) // 提交map任务
			if !working2 {                                          // coordinator关闭了，认为是任务完成了
				return
			}
			wlog.Println("Coordinator task", reply.TaskId, "has committed")
		} else if reply.TaskType == "reduce" {
			ReduceWork(reply, reducef) // 做reduce任务
			args2 := CommitArgs{WorkerId: workerId, TaskId: reply.TaskId, TaskType: "reduce"}
			reply2 := CommitReply{}
			working2 := call("Coordinator.Commit", &args2, &reply2) // 提交reduce任务
			if !working2 {                                          // coordinator关闭了，认为是任务完成了
				return
			}
			wlog.Println("reduce task", reply.TaskId, "has committed")
		} else {
			time.Sleep(time.Second) // 没有任务时候就稍等一会
		}
	}
}
func MapWork(reply RequstReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.File) // 打开文件
	if err != nil {
		wlog.Fatalln("can't open", reply.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		wlog.Fatalln("can't read", reply.File)
	}
	defer file.Close()
	kva := mapf(reply.File, string(content))          // 调用mapf函数
	intermediate := make([][]KeyValue, reply.NReduce) // 通过ihash来对kva的数据进行分组
	for i := range kva {
		index := ihash(kva[i].Key) % reply.NReduce
		intermediate[index] = append(intermediate[index], kva[i])
	}
	// 分成NReduce个临时文件，然后由Reduce任务去读取
	for i := 0; i < reply.NReduce; i++ {
		ifilename := fmt.Sprintf("mr-tmp-%d-%d", reply.TaskId, i)
		// 创建临时文件
		ifile, err := ioutil.TempFile(".", ifilename)
		if err != nil {
			wlog.Fatalln(err)
		}
		enc := json.NewEncoder(ifile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalln(err)
			}
		}
		ifile.Close()
		// 重命名，覆盖文件
		if err := os.Rename(ifile.Name(), ifilename); err != nil {
			wlog.Fatalln("cannot rename", ifilename)
		}
	}
}

func ReduceWork(reply RequstReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	// 读取之前由map任务分成各自TaskId的所有map任务的中间文件
	for mapTaskNumber := 0; mapTaskNumber < reply.NMap; mapTaskNumber++ {
		filename := fmt.Sprintf("mr-tmp-%d-%d", mapTaskNumber, reply.TaskId)
		file, err := os.Open(filename)
		if err != nil {
			wlog.Println(err)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	//排序
	sort.Sort(ByKey(intermediate))
	// 生成结果文件，也是通过临时文件然后重命名的方式
	i := 0
	oFileName := "mr-out-" + strconv.Itoa(reply.TaskId+1)
	ofile, err := ioutil.TempFile(".", oFileName)
	if err != nil {
		wlog.Fatalln(err)
	}
	defer ofile.Close()
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	err = os.Rename(ofile.Name(), oFileName)
	if err != nil {
		wlog.Fatalln(err)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

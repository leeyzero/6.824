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
	"time"
)

const (
	intermediateTempDir = "mr-tmp"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueList []KeyValue

func (a KeyValueList) Len() int           { return len(a) }
func (a KeyValueList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KeyValueList) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		task, ok := callFetchTaskCommand()
		if !ok {
			break
		}

		switch task.TaskType {
		case TASK_TYPE_MAP:
			runMapTask(mapf, task)
		case TASK_TYPE_REDUCE:
			runReduceTask(reducef, task)
		case TASK_TYPE_RETRY:
			log.Println("worker need retry")
		case TASK_TYPE_EXIT:
			log.Println("worker exit")
			return
		default:
			log.Printf("task_type[%v] invalid\n", task.TaskType)
		}

		time.Sleep(500 * time.Millisecond)
	}
}

//
// map任务
//
func runMapTask(mapf func(string, string) []KeyValue, task *TaskNode) {
	ifile, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	defer ifile.Close()
	content, err := ioutil.ReadAll(ifile)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}

	// call user map function
	kva := mapf(task.Filename, string(content))

	bucket2KVs := make(map[int][]KeyValue)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.NReduce
		bucket2KVs[bucket] = append(bucket2KVs[bucket], kv)
	}

	// create internediate temp dir if not exists
	if _, err := os.Stat(intermediateTempDir); os.IsNotExist(err) {
		os.Mkdir(intermediateTempDir, 0755)
	}

	tmpFiles := make([]*os.File, task.NReduce)
	for bucket := 0; bucket < task.NReduce; bucket++ {
		tmpFiles[bucket], err = ioutil.TempFile(intermediateTempDir, "mr-tmp-*")
		if err != nil {
			log.Fatalf("ioutil.TempFile error:%v", err)
		}

		enc := json.NewEncoder(tmpFiles[bucket])
		for _, kv := range bucket2KVs[bucket] {
			enc.Encode(&kv)
		}
	}

	for bucket, ofile := range tmpFiles {
		newFilename := fmt.Sprintf("%s/mr-%d-%d", intermediateTempDir, task.FileIndex, bucket)
		os.Rename(ofile.Name(), newFilename)
		ofile.Close()
	}

	// report master
	callReportCommand(task.TaskID, task.TaskType)
}

//
// reduce任务
//
func runReduceTask(reducef func(string, []string) string, task *TaskNode) {
	kva := []KeyValue{}
	for fileIndex := 0; fileIndex < task.NFiles; fileIndex++ {
		filename := fmt.Sprintf("%s/mr-%d-%d", intermediateTempDir, fileIndex, task.Bucket)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(KeyValueList(kva))

	ofile, err := ioutil.TempFile(intermediateTempDir, "mr-*")
	if err != nil {
		log.Fatalf("ioutil.TempFile error:%v", err)
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	newFilename := fmt.Sprintf("mr-out-%d", task.Bucket)
	os.Rename(ofile.Name(), newFilename)
	ofile.Close()

	// report to master
	callReportCommand(task.TaskID, task.TaskType)
}

//
// rpc获取任务命令
//
func callFetchTaskCommand() (*TaskNode, bool) {
	cmdReply, ok := callCommand(CMD_FETCH_TASK, "")
	if !ok {
		return nil, ok
	}

	var task TaskNode
	err := json.Unmarshal([]byte(cmdReply.Data), &task)
	if err != nil {
		log.Println("json.Unmarshal error:", err)
		return nil, false
	}

	return &task, true
}

// rpc汇报任务命令
func callReportCommand(taskID int, taskType string) bool {
	node := ReportNode{
		TaskID:   taskID,
		TaskType: taskType,
	}
	data, _ := node.Encode()
	_, ok := callCommand(CMD_REPORT_TASK, data)
	return ok
}

//
// the RPC argument and reply types are defined in rpc.go
//
func callCommand(cmd string, data string) (*CommandReply, bool) {
	args := CommandArgs{
		Cmd:  cmd,
		Data: data,
	}
	reply := CommandReply{}

	ok := call("Coordinator.Command", &args, &reply)
	if ok {
		log.Println("command.reply", reply)
		return &reply, true
	}

	return nil, false
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
		log.Fatal("rpc.DialHTTP error:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println("rpc.Call error:", err)
	return false
}

package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// 事件名定义
const (
	EVENT_FETCH_TASK   = "EVENT_FETCH_TASK"
	EVENT_REPORT_TASK  = "EVENT_REPORT_TASK"
	EVENT_TASK_TIMEOUT = "EVENT_TASK_TIMEOUT"
)

type Coordinator struct {
	// Your definitions here.

	files   []string
	nReduce int

	chEventRequest chan *EventRequest

	mapWaitingTaskQueue   *TaskQueue
	mapRunningTaskQueue   *TaskQueue
	mapCompletedTaskQueue *TaskQueue

	reduceWaitingTaskQueue   *TaskQueue
	reduceRunningTaskQueue   *TaskQueue
	reduceCompletedTaskQueue *TaskQueue

	done int32
}

type EventRequest struct {
	name string
	data string
	resp chan *EventResponse
}

type EventResponse struct {
	data string
	err  error
}

var (
	idGenerator int32
)

// GenTaskID - generate task id
func GenTaskID() int {
	return int(atomic.AddInt32(&idGenerator, 1))
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Command(args *CommandArgs, reply *CommandReply) error {
	eventName := ""
	switch args.Cmd {
	case CMD_FETCH_TASK:
		eventName = EVENT_FETCH_TASK
	case CMD_REPORT_TASK:
		eventName = EVENT_REPORT_TASK
	default:
		return fmt.Errorf("cmd[%v] not support", args.Cmd)
	}

	log.Printf("rpc cmd:%v\n", *args)

	// 发送事件请求
	resp := c.sendEvent(eventName, args.Data)
	if resp.err != nil {
		return fmt.Errorf("handle cmd[%v] error:%v", args.Cmd, resp.err)
	}

	reply.Cmd = args.Cmd
	reply.CmdID = uuid.NewString()
	reply.Data = resp.data
	return nil
}

func (c *Coordinator) eventLoop() {
	go func() {
		// 事件调度均在主循环线程中完成，避免对任务队列加锁
		for event := range c.chEventRequest {
			log.Printf("event request: %v\n", *event)

			resp := c.dispatchEvent(event)

			log.Printf("event response: %v\n", *resp)

			// 异步线程返回, 不阻塞事件循环主线程
			go func(event *EventRequest, resp *EventResponse) {
				event.resp <- resp
			}(event, resp)
		}
	}()
}

func (c *Coordinator) sendEvent(name string, data string) *EventResponse {
	event := EventRequest{
		name: name,
		data: data,
		resp: make(chan *EventResponse),
	}
	go func(event *EventRequest) {
		c.chEventRequest <- event
	}(&event)

	return <-event.resp
}

func (c *Coordinator) dispatchEvent(event *EventRequest) *EventResponse {
	var (
		data string
		err  error
	)

	switch event.name {
	case EVENT_FETCH_TASK:
		data, err = c.handleFetchTaskEvent(event.data)
	case EVENT_REPORT_TASK:
		data, err = c.handleReportTaskEvent(event.data)
	case EVENT_TASK_TIMEOUT:
		data, err = c.handleTaskTimeoutEvent(event.data)
	default:
		data, err = "", fmt.Errorf("cannot handle event[%v]", event.name)
	}

	resp := EventResponse{
		data: data,
		err:  err,
	}
	return &resp
}

func (c *Coordinator) handleFetchTaskEvent(data string) (string, error) {
	// reduce队列有待处理的任务
	if c.hasWaitingReduceTask() {
		// 分配reduce任务
		return c.dispatchReduceTask()
	}

	// reduce队列中有正在进行的任务
	if c.hasRunningReduceTask() {
		// 分配重试任务
		return c.dispatchRetryTask()
	}

	// reduce任务全部完成
	if c.isAllReduceTaskCompleted() {
		// 设置任务已完成, 并分配退出任务
		atomic.StoreInt32(&c.done, int32(1))
		return c.dispatchExitTask()
	}

	// map队列是有待处理的任务
	if c.hasWaitingMapTask() {
		// 分配map任务
		return c.dispatchMapTask()
	}

	// map队列中有正在进行的任务
	if c.hasRunningMapTask() {
		// 分配等待任务
		return c.dispatchRetryTask()
	}

	// map任务全部完成
	if c.isAllMapTaskCompleted() {
		// 将map队列中完成的任务加入到reduce队列中, 并分配一个重试任务
		c.generateReduceTasks()
		return c.dispatchRetryTask()
	}

	log.Println("some bug ocurred maybe, please check")
	return c.dispatchRetryTask()
}

func (c *Coordinator) handleReportTaskEvent(data string) (string, error) {
	rn := ReportNode{}
	err := rn.Decode(data)
	if err != nil {
		return "", err
	}

	switch rn.TaskType {
	case TASK_TYPE_MAP:
		return c.handleReportMapTask(rn.TaskID)
	case TASK_TYPE_REDUCE:
		return c.handleReportReduceTask(rn.TaskID)
	default:
	}
	return "", fmt.Errorf("handle report task_type[%v] not support", rn.TaskType)
}

func (c *Coordinator) handleReportMapTask(taskID int) (string, error) {
	task, ok := c.mapRunningTaskQueue.Take(taskID)
	if !ok {
		return "", fmt.Errorf("handle report map task[%v] is not runing", taskID)
	}

	// 添加到完成队列
	c.mapCompletedTaskQueue.Push(task)

	// 通知任务完成
	task.Done()

	return "", nil
}

func (c *Coordinator) handleReportReduceTask(taskID int) (string, error) {
	task, ok := c.reduceRunningTaskQueue.Take(taskID)
	if !ok {
		return "", fmt.Errorf("handle report reduce task[%v] is not running", taskID)
	}

	// 添加到完成队列
	c.reduceCompletedTaskQueue.Push(task)

	// 通知任务完成
	task.Done()

	return "", nil
}

// 处理任务超时事件
func (c *Coordinator) handleTaskTimeoutEvent(data string) (string, error) {
	rn := ReportNode{}
	err := rn.Decode(data)
	if err != nil {
		return "", err
	}

	switch rn.TaskType {
	case TASK_TYPE_MAP:
		return c.handleMapTaskTimeout(rn.TaskID)
	case TASK_TYPE_REDUCE:
		return c.handleReduceTaskTimeout(rn.TaskID)
	default:
	}
	return "", fmt.Errorf("handle timeout task_type[%v] not support", rn.TaskType)
}

func (c *Coordinator) handleMapTaskTimeout(taskID int) (string, error) {
	task, ok := c.mapRunningTaskQueue.Take(taskID)
	if !ok {
		return "", fmt.Errorf("handle map task[%v] timeout is not running", taskID)
	}

	// 添加到等待队列
	c.mapWaitingTaskQueue.Push(task)
	return "", nil
}

func (c *Coordinator) handleReduceTaskTimeout(taskID int) (string, error) {
	task, ok := c.reduceRunningTaskQueue.Take(taskID)
	if !ok {
		return "", fmt.Errorf("handle reduce task[%v] timeout is not running", taskID)
	}

	// 添加到等待队列
	c.reduceWaitingTaskQueue.Push(task)
	return "", nil
}

func (c *Coordinator) dispatchReduceTask() (string, error) {
	task, err := c.reduceWaitingTaskQueue.Pop()
	if err != nil {
		return "", err
	}

	data, err := task.Encode()
	if err != nil {
		return "", err
	}

	// 添加到运行队列
	c.reduceRunningTaskQueue.Push(task)

	// 开启线程等待任务完成
	c.asyncWaitTaskDone(task)

	return data, nil
}

func (c *Coordinator) dispatchMapTask() (string, error) {
	task, err := c.mapWaitingTaskQueue.Pop()
	if err != nil {
		return "", err
	}

	data, err := task.Encode()
	if err != nil {
		return "", err
	}

	// 添加到运行队列
	c.mapRunningTaskQueue.Push(task)

	// 开启线程等待任务完成
	c.asyncWaitTaskDone(task)

	return data, nil
}

func (c *Coordinator) asyncWaitTaskDone(task *TaskNode) {
	go func(task *TaskNode) {
		data, _ := task.Encode()
		for {
			select {
			case <-time.After(10 * time.Second):
				log.Printf("task_id[%v] task_type[%v] timeout, requeue\n", task.TaskID, task.TaskType)
				c.sendEvent(EVENT_TASK_TIMEOUT, data)
				return
			case <-task.done:
				return
			}
		}
	}(task)
}

func (c *Coordinator) dispatchRetryTask() (string, error) {
	task := MakeRetryTaskNode(GenTaskID())
	data, err := task.Encode()
	if err != nil {
		return "", err
	}
	return data, nil
}

func (c *Coordinator) dispatchExitTask() (string, error) {
	task := MakeExitTaskNode(GenTaskID())
	data, err := task.Encode()
	if err != nil {
		return "", err
	}
	return data, nil
}

func (c *Coordinator) hasWaitingReduceTask() bool {
	return c.reduceWaitingTaskQueue.Count() > 0
}

func (c *Coordinator) hasRunningReduceTask() bool {
	return c.reduceRunningTaskQueue.Count() > 0
}

func (c *Coordinator) isAllReduceTaskCompleted() bool {
	return c.reduceCompletedTaskQueue.Count() == c.nReduce
}

func (c *Coordinator) hasWaitingMapTask() bool {
	return c.mapWaitingTaskQueue.Count() > 0
}

func (c *Coordinator) hasRunningMapTask() bool {
	return c.mapRunningTaskQueue.Count() > 0
}

func (c *Coordinator) isAllMapTaskCompleted() bool {
	return c.mapCompletedTaskQueue.Count() == len(c.files)
}

func (c *Coordinator) generateReduceTasks() {
	for i := 0; i < c.nReduce; i++ {
		task := MakeReduceTaskNode(GenTaskID(), i, len(c.files), c.nReduce)
		c.reduceWaitingTaskQueue.Push(task)
	}
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
	// Your code here.
	return atomic.LoadInt32(&c.done) == int32(1)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:                    files,
		nReduce:                  nReduce,
		chEventRequest:           make(chan *EventRequest),
		mapWaitingTaskQueue:      NewTaskQueue(),
		mapRunningTaskQueue:      NewTaskQueue(),
		mapCompletedTaskQueue:    NewTaskQueue(),
		reduceWaitingTaskQueue:   NewTaskQueue(),
		reduceRunningTaskQueue:   NewTaskQueue(),
		reduceCompletedTaskQueue: NewTaskQueue(),
		done:                     0,
	}

	// Your code here.
	// 生成map任务，并放入等待队列中
	for fileIndex, filename := range files {
		task := MakeMapTaskNode(GenTaskID(), fileIndex, filename, len(files), nReduce)
		c.mapWaitingTaskQueue.Push(task)
	}

	// 启动事件循环
	c.eventLoop()

	c.server()
	return &c
}

// MakeMapTaskNode - create a map TaskNode
func MakeMapTaskNode(taskID int, fileIndex int, filename string, nFiles int, nReduce int) *TaskNode {
	task := TaskNode{
		TaskID:    taskID,
		TaskType:  TASK_TYPE_MAP,
		FileIndex: fileIndex,
		Filename:  filename,
		NFiles:    nFiles,
		NReduce:   nReduce,

		// 使用buffered channel, 避免调用TaskNode.Done时阻塞
		done: make(chan bool, 1),
	}
	return &task
}

// MakeReduceTaskNode - create a reduce TaskNode
func MakeReduceTaskNode(taskID int, bucket int, NFiles int, nReduce int) *TaskNode {
	task := TaskNode{
		TaskID:   taskID,
		TaskType: TASK_TYPE_REDUCE,
		NFiles:   NFiles,
		NReduce:  nReduce,
		Bucket:   bucket,

		// 使用buffered channel, 避免调用TaskNode.Done时阻塞
		done: make(chan bool, 1),
	}
	return &task
}

// MakeRetryTaskNode - create a retry TaskNode
func MakeRetryTaskNode(taskID int) *TaskNode {
	task := TaskNode{
		TaskID:   taskID,
		TaskType: TASK_TYPE_RETRY,
	}
	return &task
}

// MakeExitTaskNode - create a exit TaskNode
func MakeExitTaskNode(taskID int) *TaskNode {
	task := TaskNode{
		TaskID:   taskID,
		TaskType: TASK_TYPE_EXIT,
	}
	return &task
}

package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"encoding/json"
	"os"
	"strconv"
)

const (
	TASK_TYPE_MAP    = "TASK_TYPE_MAP"    // map任务
	TASK_TYPE_REDUCE = "TASK_TYPE_REDUCE" // reduce任务
	TASK_TYPE_RETRY  = "TASK_TYPE_RETRY"  // 重试任务
	TASK_TYPE_EXIT   = "TASK_TYPE_EXIT"   // 退出任务
)

const (
	// 请求任务命令
	CMD_FETCH_TASK = "CMD_FETCH_TASK"

	// 汇报任务命令
	CMD_REPORT_TASK = "CMD_REPORT_TASK"
)

// Add your RPC definitions here.

//
// 命令请求参数
//
type CommandArgs struct {
	Cmd  string
	Data string
}

//
// 命令请求返回
//
type CommandReply struct {
	Cmd   string
	CmdID string
	Data  string
}

//
// FETCH_TASK命令data数据结构
//
type TaskNode struct {
	TaskID    int    `json:"task_id"`
	TaskType  string `json:"task_type"`
	FileIndex int    `json:"file_index"`
	Filename  string `json:"filename"`
	NFiles    int    `json:"nfiles"`
	NReduce   int    `json:"nreduce"`
	Bucket    int    `json:"bucket"`

	// 完成通知
	done chan bool
}

//
// REPORT_TASK命令data数据结构
//
type ReportNode struct {
	TaskID   int    `json:"task_id"`
	TaskType string `json:"task_type"`
}

func (t *TaskNode) Encode() (string, error) {
	data, err := json.Marshal(t)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (t *TaskNode) Decode(content string) error {
	return json.Unmarshal([]byte(content), t)
}

func (t *TaskNode) Done() {
	t.done <- true
}

func (r *ReportNode) Encode() (string, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (r *ReportNode) Decode(content string) error {
	return json.Unmarshal([]byte(content), r)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

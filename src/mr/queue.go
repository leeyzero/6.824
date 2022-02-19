package mr

import (
	"errors"
	"fmt"
)

// TaskQueue - 任务队列
type TaskQueue struct {
	m map[int]*TaskNode
}

// NewTaskQueue - 创建一个任务队列
func NewTaskQueue() *TaskQueue {
	return &TaskQueue{
		m: make(map[int]*TaskNode),
	}
}

// Pop - 随机获取一个任务
func (t *TaskQueue) Pop() (*TaskNode, error) {
	var taskID int
	for k, _ := range t.m {
		taskID = k
		break
	}

	v, ok := t.Take(taskID)
	if !ok {
		return nil, fmt.Errorf("queue is empty")
	}
	return v, nil
}

// Push - 任务加入到队列
func (t *TaskQueue) Push(task *TaskNode) error {
	if _, ok := t.m[task.TaskID]; ok {
		return errors.New("task already exists")
	}

	t.m[task.TaskID] = task
	return nil
}

// Take - 根据TaskID获取任务
func (t *TaskQueue) Take(taskID int) (*TaskNode, bool) {
	v, ok := t.m[taskID]
	if !ok {
		return nil, false
	}

	delete(t.m, taskID)
	return v, true
}

// Count - 任务数
func (t *TaskQueue) Count() int {
	return len(t.m)
}

package mr

import (
	"log"
	"time"
)

// TaskType 表示任务的类型
type TaskType int

// 下面定义了常量用于表示各种任务类型
const (
	MapTask    TaskType = iota // MapTask 代表映射任务类型
	ReduceTask TaskType = iota // ReduceTask 代表归并任务类型
	ExitTask   TaskType = iota // ExitTask 代表退出任务类型
	WaitTask   TaskType = iota // WaitTask 代表等待任务类型
)

// Task 类包含任务的类型、标识、文件名和中间任务相关的信息
type Task struct {
	TaskType   TaskType
	TaskId     int
	FileNames  []string
	MiddleTask int
}

// TaskStatus 表示任务的状态
type TaskStatus int

// 下面定义了常量用于表示各种任务状态
const (
	Unassigned TaskStatus = iota // Unassigned 代表未分配的任务状态
	Assigned   TaskStatus = iota // Assigned 代表已分配的任务状态
	Finished   TaskStatus = iota // Finished 代表已完成的任务状态
)

// TaskMetaData 表示任务的元数据，包含任务状态、开始时间和任务对象等
type TaskMetaData struct {
	TaskStatus TaskStatus
	StartTime  time.Time
	Task       *Task
}

// TaskSet 是一组任务的集合，包含映射任务和归并任务
type TaskSet struct {
	mapTaskMap    map[int]*TaskMetaData
	reduceTaskMap map[int]*TaskMetaData
}

// NewTaskSet 创建新的任务集合实例，并初始化 mapTaskMap 和 reduceTaskMap
func NewTaskSet() *TaskSet {
	return &TaskSet{
		mapTaskMap:    make(map[int]*TaskMetaData),
		reduceTaskMap: make(map[int]*TaskMetaData),
	}
}

// NewTask 创建一个新的任务实例，并设定任务类型、标识、文件名和中间任务的值
func NewTask(taskType TaskType, TaskId int, FileNames []string, MiddleTask int) *Task {
	return &Task{
		TaskType:   taskType,
		TaskId:     TaskId,
		FileNames:  FileNames,
		MiddleTask: MiddleTask,
	}
}

// NewTaskMetaData 创建一个给定任务的新的任务元数据对象
func NewTaskMetaData(task *Task) *TaskMetaData {
	return &TaskMetaData{
		TaskStatus: Unassigned,
		Task:       task,
	}
}

// RegisterTask 在任务集合中注册一个任务，如果任务类型是映射任务，它添加任务标识和任务元数据到 mapTaskMap
// 如果任务类型是归并任务，它添加任务标识和任务元数据到 reduceTaskMap，否则，它会触发 panic 提示 "Invalid task type."
func (ts *TaskSet) RegisterTask(task *Task) {
	taskMetadata := NewTaskMetaData(task)
	if task.TaskType == MapTask {
		ts.mapTaskMap[task.TaskId] = taskMetadata
	} else if task.TaskType == ReduceTask {
		ts.reduceTaskMap[task.TaskId] = taskMetadata
	} else {
		log.Panic("Invalid task type.")
	}
}

// StartTask 在任务集合中启动一个任务，设定任务的开始时间和状态为 Assigned，并返回是否成功启动的布尔值
func (ts *TaskSet) StartTask(task *Task) bool {
	var taskMetadata *TaskMetaData
	if task.TaskType == MapTask {
		taskMetadata = ts.mapTaskMap[task.TaskId]
	} else if task.TaskType == ReduceTask {
		taskMetadata = ts.reduceTaskMap[task.TaskId]
	} else {
		log.Panic("Invalid task type.")
		return false
	}
	taskMetadata.StartTime = time.Now()
	taskMetadata.TaskStatus = Assigned
	return true
}

// CompleteTask 在任务集合中标记一个任务完成
func (ts *TaskSet) CompleteTask(task *Task) bool {
	var taskMetadata *TaskMetaData
	if task.TaskType == MapTask {
		taskMetadata = ts.mapTaskMap[task.TaskId]
	} else if task.TaskType == ReduceTask {
		taskMetadata = ts.reduceTaskMap[task.TaskId]
	} else {
		log.Panic("Invalid task type.")
	}
	if taskMetadata.TaskStatus == Finished {
		log.Printf("Task has already been finished.Task: %v\n", taskMetadata.Task)
		return false
	}
	taskMetadata.TaskStatus = Finished
	return true
}

// IsAllMapTaskCompleted 检查任务集合中的所有映射任务是否已完成
func (ts *TaskSet) IsAllMapTaskCompleted() bool {
	for _, taskMetadata := range ts.mapTaskMap {
		if taskMetadata.TaskStatus != Finished {
			return false
		}
	}
	return true
}

// IsAllReduceTaskCompleted 检查任务集合中的所有归并任务是否已完成
func (ts *TaskSet) IsAllReduceTaskCompleted() bool {
	for _, taskMetadata := range ts.reduceTaskMap {
		if taskMetadata.TaskStatus != Finished {
			return false
		}
	}
	return true
}

// IsAllTaskCompleted 检查任务集合中的所有任务是否已完成
func (ts *TaskSet) IsAllTaskCompleted() bool {
	return ts.IsAllMapTaskCompleted() && ts.IsAllReduceTaskCompleted()
}

const TimeOutDuration time.Duration = 10

// IsMapTaskTimeOut 检查任务集中是否有映射任务超时，并返回超时任务列表
func (ts *TaskSet) IsMapTaskTimeOut() []*Task {
	var ret []*Task
	for _, taskMetadata := range ts.mapTaskMap {
		if taskMetadata.TaskStatus == Assigned && time.Since(taskMetadata.StartTime) > TimeOutDuration*time.Second {
			ret = append(ret, taskMetadata.Task)
		}
	}
	return ret
}

// IsReduceTaskTimeOut 检查任务集中是否有归并任务超时，并返回超时任务列表
func (ts *TaskSet) IsReduceTaskTimeOut() []*Task {
	var ret []*Task
	for _, taskMetadata := range ts.reduceTaskMap {
		if taskMetadata.TaskStatus == Assigned && time.Since(taskMetadata.StartTime) > TimeOutDuration*time.Second {
			ret = append(ret, taskMetadata.Task)
		}
	}
	return ret
}

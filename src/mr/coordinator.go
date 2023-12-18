package mr

import (
	"fmt"
	"github.com/google/uuid"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type CoordinatorStatus int

const (
	Mapping CoordinatorStatus = iota
	Reducing
	Exiting
)

type Coordinator struct {
	// Your definitions here.
	Id          uuid.UUID
	Mutex       sync.Mutex
	Status      CoordinatorStatus
	MapCount    int
	ReduceCount int
	TaskChannel chan *Task
	TaskSet     *TaskSet
	FileName    []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

const waitTimeBeforeExiting time.Duration = 5

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.Mutex.Lock()
	if c.Status == Exiting {
		ret = true
		log.Println("Coordinator is waiting to exit.")
		time.Sleep(waitTimeBeforeExiting * time.Second)
		log.Println("Coordinator is exiting.")
	}
	c.Mutex.Unlock()
	return ret
}

func (c *Coordinator) RegisterTask() {
	for index, fileName := range c.FileName {
		task := NewTask(MapTask, index, []string{fileName}, c.ReduceCount)
		c.TaskSet.RegisterTask(task)
		go func() { c.TaskChannel <- task }()
	}
	log.Println("All tasks are registered.")
}

func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	log.Printf("Coordinator %v received FetchTask request from worker %v.\n", c.Id, args.NodeId)
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	select {
	case task := <-c.TaskChannel:
		c.TaskSet.StartTask(task)
		reply.Task = task
		reply.Msg = "Task is assigned."
	default:
		log.Printf("Coordinator %v has no task to assign.\n", c.Id)
		if c.CheckStatus() {
			c.MoveToNextStage()
		}
		if c.Status == Exiting {
			reply.Msg = "Coordinator is exiting. Exit task is assigned."
			reply.Task = &Task{
				TaskType: ExitTask,
			}
		} else {
			reply.Msg = "Coordinator is waiting. Wait task is assigned."
			reply.Task = &Task{
				TaskType: WaitTask,
			}
		}
	}
	return nil
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	log.Printf("Coordinator %v received SubmitTask request from worker %v. Task: %v.\n", c.Id, args.NodeId, args.Task.TaskId)
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	reply.Msg = "Task is received."
	if c.TaskSet.CompleteTask(args.Task) {
		log.Printf("Coordinator %v completed task %v.\n", c.Id, args.Task.TaskId)
	} else {
		log.Printf("Coordinator %v received repeated completion of task %v.\n", c.Id, args.Task.TaskId)
	}
	return nil
}

func (c *Coordinator) CheckStatus() bool {
	switch c.Status {
	case Mapping:
		return c.TaskSet.IsAllMapTaskCompleted()
	case Reducing:
		return c.TaskSet.IsAllReduceTaskCompleted()
	case Exiting:
		return true
	default:
		log.Panic("Invalid coordinator status.")
		return false
	}
}

const IntermediateNameTemplate string = "mr-tmp-%d-%d"

func (c *Coordinator) RegisterReduceTask() {
	for i := 0; i < c.ReduceCount; i++ {
		task := &Task{
			TaskType:  ReduceTask,
			TaskId:    i,
			FileNames: []string{},
		}

		for j := 0; j < c.MapCount; j++ {
			fileName := fmt.Sprintf(IntermediateNameTemplate, j, i)
			task.FileNames = append(task.FileNames, fileName)
		}
		c.TaskSet.RegisterTask(task)
		go func() { c.TaskChannel <- task }()
	}
	log.Println("All reduce tasks are registered.")
}

func (c *Coordinator) MoveToNextStage() {
	switch c.Status {
	case Mapping:
		log.Printf("Coordinator %v is moving to reducing stage.\n", c.Id)
		c.Status = Reducing
		c.RegisterReduceTask()
	case Reducing:
		log.Printf("Coordinator %v is moving to exiting stage.\n", c.Id)
		c.Status = Exiting
	case Exiting:
		log.Println("Coordinator is in exiting stage.")
	default:
		log.Panic("Invalid coordinator status.")
	}
}

const TimeOutCheckInterval time.Duration = 2

func (c *Coordinator) TimeOutDetection() {
	for {
		c.Mutex.Lock()
		var timeOutTask []*Task
		switch c.Status {
		case Mapping:
			timeOutTask = c.TaskSet.IsMapTaskTimeOut()
		case Reducing:
			timeOutTask = c.TaskSet.IsReduceTaskTimeOut()
		case Exiting:
			c.Mutex.Unlock()
			return
		}
		for _, task := range timeOutTask {
			log.Printf("Task %v is time out.\n", task.TaskId)
			c.TaskSet.RegisterTask(task)
			lTask := task
			go func() { c.TaskChannel <- lTask }()
		}
		c.Mutex.Unlock()
		time.Sleep(TimeOutCheckInterval * time.Second)
	}
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	logfile, err := os.OpenFile("logfile.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer logfile.Close()
	// 将日志输出重定向到文件
	log.SetOutput(logfile)

	c := Coordinator{
		Id:          uuid.New(),
		Status:      Mapping,
		MapCount:    len(files),
		FileName:    files,
		TaskSet:     NewTaskSet(),
		TaskChannel: make(chan *Task),
		ReduceCount: nReduce,
	}

	log.Printf("Coordinator %v is created.\n", c.Id)
	// Your code here.

	c.RegisterTask()
	go c.TimeOutDetection()
	c.server()
	return &c
}

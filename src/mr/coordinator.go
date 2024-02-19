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

// CoordinatorStatus is a custom type representing the status of a coordinator.
//
// It has the following possible values:
// - Mapping: Indicates that the coordinator is currently performing mapping tasks.
// - Reducing: Indicates that the coordinator is currently performing reducing tasks.
// - Idle: Indicates that the coordinator is idle and not performing any tasks.
type CoordinatorStatus int

// Mapping represents the status of a coordinator during the mapping phase.
const (
	Mapping CoordinatorStatus = iota
	Reducing
	Exiting
)

// Coordinator represents the coordinator in a distributed computing system.
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

// Example is a method of Coordinator that takes ExampleArgs as input and returns ExampleReply.
// It adds 1 to the value of X from ExampleArgs and assigns it to Y in ExampleReply.
// It always returns nil as error.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// server starts the RPC server for the Coordinator.
// It registers the Coordinator instance with the rpc package, sets up the HTTP handler, removes any existing socket file,
// listens on the socket, and starts serving RPC requests on a separate goroutine.
// The Coordinator instance must be registered before calling this method.
// Usage example:
//
//	coordinator := &Coordinator{}
//	coordinator.server()
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

// waitTimeBeforeExiting is the duration of time that the Coordinator will wait before exiting when its status is Exiting.
const waitTimeBeforeExiting time.Duration = 5

// Done checks if the Coordinator is in the Exiting state. If it is, the function waits for a certain amount of time before returning true. If the Coordinator is not in the Exiting state
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

// RegisterTask registers tasks for the coordinator.
// It creates a new task for each file in the FileName slice of the coordinator.
// Each task is of type MapTask and has an index and fileName.
// The task is then registered in the TaskSet and added to the TaskChannel for processing.
// After all tasks are registered, it prints a log message indicating that all tasks are registered.
func (c *Coordinator) RegisterTask() {
	for index, fileName := range c.FileName {
		task := NewTask(MapTask, index, []string{fileName}, c.ReduceCount)
		c.TaskSet.RegisterTask(task)
		go func() { c.TaskChannel <- task }()
	}
	log.Println("All tasks are registered.")
}

// FetchTask handles the FetchTask RPC request from a worker.
// It assigns a task to the worker if there is one available in the TaskChannel.
// If there are no tasks available, it checks the status of the coordinator and assigns either an ExitTask or a WaitTask.
// If the coordinator is in Exiting status, an ExitTask is assigned, otherwise a WaitTask is assigned.
// It logs the coordinator ID and worker ID for monitoring purposes.
//
// Args:
// - args: FetchTaskArgs struct containing message and NodeId.
// - reply: FetchTaskReply struct containing message and a Task pointer.
//
// Returns:
// - error: Returns nil in all cases.
//
// Example:
//
//	c := &Coordinator{}
//	fetchArgs := &FetchTaskArgs{
//	    Msg:    "FetchTask",
//	    NodeId: "worker1",
//	}
//	fetchReply := &FetchTaskReply{}
//	c.FetchTask(fetchArgs, fetchReply)
//	fmt.Println(fetchReply.Task.TaskType) // Output: 0 (MapTask)
//	fmt.Println(fetchReply.Task.TaskId) // Output: 1
//
// Note: The TaskChannel must be previously populated with tasks using RegisterTask method.
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

// SubmitTask handles the submission of a task by a worker to the coordinator.
// It logs the received task information and acquires a lock on the coordinator.
// It sets the "Msg" field of the reply to "Task is received."
// It checks if the task is already completed, logs the appropriate message, and returns.
// Otherwise, it marks the task as completed in the task set and logs the completion.
// Finally, it releases the lock and returns without any errors.
// Note: This method is part of the Coordinator struct.
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

// CheckStatus checks the current status of the Coordinator and returns a boolean indicating if the status check passes.
// The Coordinator's status can be one of the following:
// - Mapping: It checks if all map tasks are completed.
// - Reducing: It checks if all reduce tasks are completed.
// - Exiting: It always returns true.
// If the status is invalid, it raises a panic.
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

// IntermediateNameTemplate is a constant used to generate intermediate file names during the Map phase of a task in the coordinator.
// Example usage:
//
//	func (c *Coordinator) RegisterReduceTask() {
//		for i := 0; i < c.ReduceCount; i++ {
//			task := &Task{
//				TaskType:  ReduceTask,
//				TaskId:    i,
//				FileNames: []string{},
//			}
//			for j := 0; j < c.MapCount; j++ {
//				fileName := fmt.Sprintf(IntermediateNameTemplate, j, i)
//				task.FileNames = append(task.FileNames, fileName)
//			}
//			c.TaskSet.RegisterTask(task)
//			go func() { c.TaskChannel <- task }()
//		}
//		log.Println("All reduce tasks are registered.")
//	}
//
//	func ProcessMapTask(mapf func(string, string) []KeyValue, task *Task) {
//		fileName := task.FileNames[0]
//		log.Printf("Worker %v is processing map task %v.\n", Id, task.TaskId)
//		file, err := os.Open(fileName)
//		if err != nil {
//			log.Fatalf("cannot open %v", fileName)
//		}
//		content, err := io.ReadAll(file)
//		if err != nil {
//			log.Fatalf("cannot read %v", fileName)
//		}
//		file.Close()
//		kv := mapf(fileName, string(content))
//		intermediate := make([][]KeyValue, task.MiddleTask)
//		for i := 0; i < task.MiddleTask; i++ {
//			intermediate[i] = []KeyValue{}
//		}
//		for _, pair := range kv {
//			index := ihash(pair.Key) % task.MiddleTask
//			intermediate[index] = append(intermediate[index], pair)
//		}
//		for i := 0; i < task.MiddleTask; i++ {
//			intermediateFileName := fmt.Sprintf(IntermediateNameTemplate, task.TaskId, i)
//			tmp, err := os.Create(intermediateFileName)
//			if err != nil {
//				log.Fatalf("cannot create temp file")
//			}
//			enc := json.NewEncoder(tmp)
//			for _, pair := range intermediate[i] {
//				err := enc.Encode(pair)
//				if err != nil {
//					log.Fatalf("cannot encode %v", pair)
//				}
//			}
//		}
//	}
//
// IntermediateNameTemplate will be replaced with the actual file name template during runtime.
const IntermediateNameTemplate string = "mr-tmp-%d-%d"

// RegisterReduceTask registers reduce tasks in the coordinator. It creates a task for each reduce task
// and appends the intermediate file names to the task. The task is then added to the task set and sent to the task channel.
// Finally, it logs a message indicating that all reduce tasks are registered.
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

// MoveToNextStage moves the Coordinator to the next stage based on its current status.
// It updates the Coordinator's status and performs the necessary actions for the next stage.
// If the current status is Mapping, it moves to the Reducing stage by registering and assigning
// Reduce tasks to workers. If the current status is Reducing, it moves to the Exiting stage.
// If the current status is Exiting, it remains in the Exiting stage. If the current status is
// in an invalid state, it logs a panic event.
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

// TimeOutCheckInterval is a constant representing the interval in seconds for checking timeout tasks in the Coordinator.
const TimeOutCheckInterval time.Duration = 2

// TimeOutDetection periodically checks for any tasks that have timed out and re-assigns them.
// The function uses a for loop to continuously check for timed-out tasks.
// It locks the mutex to prevent concurrent access to the task set.
// It checks the current status of the coordinator and retrieves the list of timed-out tasks accordingly.
// For each timed-out task, it logs a message indicating the task ID and registers the task in the task set.
// It then creates a local copy of the task and sends it to the task channel using a goroutine.
// After processing all timed-out tasks, it releases the mutex and pauses for a duration specified by TimeOutCheckInterval.
// The function does not return any value.
// Example:
//
// c := &Coordinator{}
// go c.TimeOutDetection()
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

// MakeCoordinator creates a new Coordinator and initializes its fields.
// It opens a log file and redirects the log output to that file.
// It then creates a Coordinator object `c` and sets its fields based on the provided arguments.
// It registers the tasks for mapping and starts a goroutine to handle task timeouts.
// Finally, it starts the RPC server and returns the pointer to the created Coordinator object.
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

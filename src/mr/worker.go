package mr

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue represents a key-value pair.
type KeyValue struct {
	Key   string
	Value string
}

// ihash is a function that calculates the hash value of a given key using the FNV-1a hashing algorithm. It takes a string key as input and returns an integer hash value.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Id is a globally unique identifier generated using the UUID library.
// It is used to identify a worker node in a distributed system.
// Example usage can be found in the Worker, FetchTask, SubmitTask, ProcessMapTask, ProcessReduceTask, and ReadIntermediateFile functions.
var Id uuid.UUID = uuid.New()

// ProcessedTaskCount stores the number of tasks that have been processed.
// It is initially set to 0 and gets updated as tasks are completed.
var ProcessedTaskCount int = 0

// TaskStartTime represents the start time of a task.
// TaskStartTime is used in the SubmitTask function to calculate the elapsed time
// between the task start and completion.
// Example usage:
//
//	arg := SubmitTaskArgs{
//	    Msg:    fmt.Sprintf("Task %v completed. Cost time %v ms", task.TaskId, time.Since(TaskStartTime).Milliseconds()),
//	    NodeId: Id,
//	    Task:   task,
//	}
//
// reply := SubmitTaskReply{}
// retryCount := 0
//
//	for retryCount < maxRetryCount {
//	    if call("Coordinator.SubmitTask", &arg, &reply) {
//	        log.Printf("Worker %v submitted task %v from Coordinator.\n", Id, task.TaskId)
//	        return true
//	    } else {
//	        retryCount++
//	        log.Printf("Worker %v failed to submit task to Coordinator: %v.\n", Id, reply.Msg)
//	    }
//	}
//
// log.Panicf("Worker %v failed to receive task from Coordinator.\n", Id)
// return false
var TaskStartTime time.Time

// WaitInterval specifies the duration in seconds to wait before performing an action.
const WaitInterval time.Duration = 3

// Your worker implementation here.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	log.Printf("Worker %v is running\n", Id)

	for {
		task := FetchTask()
		if task == nil {
			log.Printf("Worker %v received nil task\n", Id)
			return
		}

		ProcessedTaskCount++
		TaskStartTime = time.Now()
		switch task.TaskType {
		case WaitTask:
			log.Printf("Worker %v received wait task %v\n", Id, task.TaskId)
			ProcessedTaskCount--
			time.Sleep(WaitInterval * time.Second)
		case MapTask:
			log.Printf("Worker %v received map task %v\n", Id, task.TaskId)
			ProcessMapTask(mapf, task)
			SubmitTask(task)
		case ReduceTask:
			log.Printf("Worker %v received reduce task %v\n", Id, task.TaskId)
			ProcessReduceTask(reducef, task)
			SubmitTask(task)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// maxRetryCount is a constant representing the maximum number of retry attempts.
// It is used in the FetchTask and SubmitTask functions to control the number of retries.
// If the call to the Coordinator fails, the functions will retry up to maxRetryCount times.
const maxRetryCount int = 5

// FetchTask retrieves a task from the Coordinator.
// It sends a FetchTaskArgs struct to the Coordinator with a message and the worker's node ID.
// It retries fetching a task a maximum of maxRetryCount times if the initial attempt fails.
// If a task is received, it logs a confirmation message and returns the retrieved task.
// If all attempts fail, it panics and logs an error message.
// It returns nil if no task is received.
func FetchTask() *Task {
	arg := FetchTaskArgs{
		Msg:    fmt.Sprintf("Completed %v tasks.", ProcessedTaskCount),
		NodeId: Id,
	}
	reply := FetchTaskReply{}
	retryCount := 0

	for retryCount < maxRetryCount {
		if call("Coordinator.FetchTask", &arg, &reply) {
			log.Printf("Worker %v received task %v from Coordinator.\n", Id, reply.Task.TaskId)
			return reply.Task
		} else {
			retryCount++
			log.Printf("Worker %v failed to receive task from Coordinator %v.\n", Id, reply)
		}
	}
	log.Panicf("Worker %v failed to receive task from Coordinator.\n", Id)
	return nil
}

// SubmitTask submits a task to the Coordinator and returns true if the submission was successful, false otherwise.
// It constructs a SubmitTaskArgs struct with the task information and calls the "Coordinator.SubmitTask" RPC method.
// If the submission fails, it retries up to a maximum of maxRetryCount times.
// If all attempts fail, it panics with a log message.
// The function also logs the submission status.
func SubmitTask(task *Task) bool {
	arg := SubmitTaskArgs{
		Msg:    fmt.Sprintf("Task %v completed. Cost time %v ms", task.TaskId, time.Since(TaskStartTime).Milliseconds()),
		NodeId: Id,
		Task:   task,
	}
	reply := SubmitTaskReply{}
	retryCount := 0

	for retryCount < maxRetryCount {
		if call("Coordinator.SubmitTask", &arg, &reply) {
			log.Printf("Worker %v submitted task %v from Coordinator.\n", Id, task.TaskId)
			return true
		} else {
			retryCount++
			log.Printf("Worker %v failed to submit task to Coordinator: %v.\n", Id, reply.Msg)
		}
	}
	log.Panicf("Worker %v failed to receive task from Coordinator.\n", Id)
	return false
}

// ProcessMapTask processes a map task by performing the following steps:
// 1. Opens the file specified in the task.
// 2. Reads the content of the file.
// 3. Calls the map function provided as an argument, passing the filename and content as parameters.
// 4. Generates a hash index for each KeyValue emitted by the map function.
// 5. Divides the KeyValue pairs into intermediate slices based on the hash index.
// 6. Creates intermediate files for each slice and encodes the KeyValue pairs into JSON format.
//
// Parameters:
// - mapf: The map function to be called for each file.
// - task: The map task to be processed, containing the file names and middle task count.
//
// Note: This function does not return any value, but it performs the necessary steps to generate
// intermediate files for further reduce tasks.
func ProcessMapTask(mapf func(string, string) []KeyValue, task *Task) {
	fileName := task.FileNames[0]
	log.Printf("Worker %v is processing map task %v.\n", Id, task.TaskId)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kv := mapf(fileName, string(content))
	intermediate := make([][]KeyValue, task.MiddleTask)
	for i := 0; i < task.MiddleTask; i++ {
		intermediate[i] = []KeyValue{}
	}

	for _, pair := range kv {
		index := ihash(pair.Key) % task.MiddleTask
		intermediate[index] = append(intermediate[index], pair)
	}

	for i := 0; i < task.MiddleTask; i++ {
		intermediateFileName := fmt.Sprintf(IntermediateNameTemplate, task.TaskId, i)
		tmp, err := os.Create(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot create temp file")
		}
		enc := json.NewEncoder(tmp)

		for _, pair := range intermediate[i] {
			err := enc.Encode(pair)
			if err != nil {
				log.Fatalf("cannot encode %v", pair)
			}
		}

	}

}

// ByKey
//
// ByKey is a user-defined type that represents a slice of KeyValue structs. It is used for sorting the slice by the Key field in ascending order.
//
// Example usage:
//
//	func (a ByKey) Len() int           { return len(a) }
//	func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
//	func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
//	intermediate := []KeyValue{}
//	sort.Sort(ByKey(intermediate))
//
// Notes:
// - The ByKey type should be used to sort a slice of KeyValue structs by the Key field.
// - The Len() method returns the length of the ByKey slice.
// - The Swap() method is used to swap two elements in the ByKey slice.
// - The Less() method compares two elements in the ByKey slice and returns true if the element at index i is less than the element at index j.
// - The ByKey slice can be sorted using the sort.Sort() function.
type ByKey []KeyValue

// Len returns the number of elements in the ByKey slice.
// It implements the Len method of the sort.Interface interface.
func (a ByKey) Len() int { return len(a) }

// Swap swaps the elements at indexes i and j in the ByKey slice.
// It modifies the slice in-place.
// After calling the Swap method, the element previously at index i will be at index j,
// and the element previously at index j will be at index i.
func (a ByKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less is a method of the ByKey type. It compares two elements of ByKey by their Key field and returns true if the Key of the element at index i is less than the Key of the element
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// OutputNameTemplate defines the template for generating the output file name in the ProcessReduceTask function.
// Usage example:
//
//	reduceFileName := fmt.Sprintf(OutputNameTemplate, task.TaskId)
//
// Declaration: const OutputNameTemplate string = "mr-out-%d"
const OutputNameTemplate string = "mr-out-%d"

// ProcessReduceTask processes a reduce task by reading the intermediate files,
// sorting the key-value pairs by key, applying the reduce function to each key
// with its corresponding values, and writing the result to the output file.
//
// Parameters:
//
//	reducef: The reduce function to apply to each key with its values.
//	task: The reduce task to process.
//
// It first reads all the intermediate files specified in the task and merges
// the key-value pairs into a single slice called intermediate. The intermediate
// slice is then sorted by key using the ByKey sorter.
//
// Next, it opens the output file using the reduce task id and creates a writer.
// Then, it iterates over the intermediate slice and groups all values with the
// same key together. For each key, it calls the reduce function with the key
// and its corresponding values, and writes the output to the output file.
//
// Finally, it closes the output file.
func ProcessReduceTask(reducef func(string, []string) string, task *Task) {
	log.Printf("Worker %v is processing reduce task %v.\n", Id, task.TaskId)
	intermediate := []KeyValue{}
	for _, fileName := range task.FileNames {
		intermediate = append(intermediate, ReadIntermediateFile(fileName)...)
	}
	sort.Sort(ByKey(intermediate))

	reduceFileName := fmt.Sprintf(OutputNameTemplate, task.TaskId)
	ofile, err := os.Create(reduceFileName)
	if err != nil {
		log.Fatalf("cannot create %v", reduceFileName)
	}
	i := 0
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
}

// ReadIntermediateFile reads the contents of the intermediate file specified by fileName.
// It decodes each KeyValue object in the file and appends them to a slice of KeyValue, kva.
// If there is any decoding error, it logs an error message and stops reading the file.
// Finally, it returns the slice of KeyValue, kva.
func ReadIntermediateFile(fileName string) []KeyValue {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	dec := json.NewDecoder(file)
	kva := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			log.Printf("Worker %v failed to decode intermediate file %v.\n", Id, fileName)
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

// declare an argument structure.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// call is a function that makes an RPC call to the coordinator using the specified RPC name, arguments, and reply interface.
// It establishes a connection to the coordinator using the unix socket returned by the coordinatorSock function.
// If the connection is successful, it calls the specified RPC method on the coordinator with the provided arguments.
// If the RPC call is successful, it returns true. Otherwise, it prints the error and returns false.
//
// Usage Example:
//
//	call("Coordinator.FetchTask", &arg, &reply)
//	This example demonstrates making an RPC call to the coordinator's FetchTask method with the provided arguments and reply.
//
//	call("Coordinator.SubmitTask", &arg, &reply)
//	This example demonstrates making an RPC call to the coordinator's SubmitTask method with the provided arguments and reply.
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

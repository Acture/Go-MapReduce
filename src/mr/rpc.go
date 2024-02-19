package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"github.com/google/uuid"
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// FetchTaskArgs 用于表示获取数据的任务。包含描述任务的消息和节点的唯一标识符。
type FetchTaskArgs struct {
	Msg    string
	NodeId uuid.UUID
}

// FetchTaskReply 用于表示获取任务后收到的响应。
type FetchTaskReply struct {
	Msg  string
	Task *Task
}

// SubmitTaskArgs 用于提交处理的任务。其中包含关于任务的附加信息，需要处理的 Task，以及处理任务的节点的唯一标识符。
type SubmitTaskArgs struct {
	Msg    string
	Task   *Task
	NodeId uuid.UUID
}

// SubmitTaskReply 表示提交任务后的响应消息。
type SubmitTaskReply struct {
	Msg string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

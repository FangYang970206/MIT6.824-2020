package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
//type mapResultArgs struct {
//}
//
//type mapResultReply struct {
//}

type TaskState int

const (
	MapState    TaskState = 0
	ReduceState TaskState = 1
	StopState   TaskState = 2
	WaitState   TaskState = 3
)

type WorkerTask struct {
	MapID          int
	ReduceID       int
	ReduceNum      int
	MapNum         int
	State          TaskState
	FileName       string
	MapFunction    func(string, string) []KeyValue
	ReduceFunction func(string, []string) string
}

type WorkerReportArgs struct {
	MapID     int
	ReduceID  int
	State     TaskState
	IsSuccess bool
}

type WorkerReportReply struct {
}

type CreateWorkerArgs struct {
}

type Reply struct {
	Filename string
	Content  string
}

//type ExampleArgs struct {
//	X int
//}
//
//type ExampleReply struct {
//	Y int
//}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

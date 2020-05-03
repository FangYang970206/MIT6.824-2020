package mr

import (
	"os"
	"strconv"
)

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
	MapTaskCnt     int
	ReduceTaskCnt  int
	State          TaskState
	FileName       string
	MapFunction    func(string, string) []KeyValue
	ReduceFunction func(string, []string) string
}

type WorkerReportArgs struct {
	MapID         int
	ReduceID      int
	State         TaskState
	IsSuccess     bool
	MapTaskCnt    int
	ReduceTaskCnt int
}

type NoReply struct {
}

type NoArgs struct {
}

type MasterDoneReply struct {
	Done bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

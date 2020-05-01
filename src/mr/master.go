package mr

import (
	"io/ioutil"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Flag struct {
	processing bool
	finished   bool
}

type Master struct {
	// Your definitions here.
	FileNames     []string
	MapFlags      []Flag
	ReduceFlags   []Flag
	MapAllDone    bool
	ReduceALLDone bool
	MapNum        int
	ReduceNum     int
	Mut           sync.Mutex
	Taskchans     chan WorkerTask
}

//func (m *Master) mapResult(arg *mapResultArgs, reply *mapResultReply) error {
//	return nil
//}

func (m *Master) CreateWorkerTask(args *CreateWorkerArgs, workerTask *WorkerTask) error {
	m.Mut.Lock()
	defer m.Mut.Unlock()
	if !m.MapAllDone {
		for idx := 0; idx < m.MapNum; idx += 1 {
			if !m.MapFlags[idx].processing && !m.MapFlags[idx].finished {
				workerTask.ReduceNum = m.ReduceNum
				workerTask.State = MapState
				workerTask.MapID = idx
				workerTask.FileName = m.FileNames[idx]
				m.MapFlags[idx].processing = true
				return nil
			}
		}
		workerTask.State = WaitState
		return nil
	}
	if !m.ReduceALLDone {
		for idx := 0; idx < m.ReduceNum; idx += 1 {
			if !m.ReduceFlags[idx].processing && !m.ReduceFlags[idx].finished {
				workerTask.State = ReduceState
				workerTask.ReduceID = idx
				m.ReduceFlags[idx].processing = true
				return nil
			}
		}
		workerTask.State = WaitState
		return nil
	}
	workerTask.State = StopState
	return nil
}

func (m *Master) HandlerWorkerReport(wr *WorkerReportArgs, task *WorkerReportReply) error {
	m.Mut.Lock()
	defer m.Mut.Unlock()
	if wr.IsSuccess {
		if wr.State == MapState {

		}
	}
}

// Your code here -- RPC handlers for the worker to call.
//func (m *Master) HandlerWorker(arg *Args, reply *Reply) error {
//	m.mut.Lock()
//	defer m.mut.Unlock()
//	for i := 0; i < m.fileNumber; i++ {
//		if !m.finished[i] && !m.processing[i] {
//			m.processing[i] = true
//			m.fileToPID[i] = arg.Pid
//			reply.Content = m.files[i]
//			reply.Filename = m.filenames[i]
//			return nil
//		}
//	}
//	return nil
//}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.MapAllDone && m.ReduceALLDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{FileNames: files,
		MapFlags:      make([]Flag, len(files), len(files)),
		ReduceFlags:   make([]Flag, nReduce, nReduce),
		MapNum:        len(files),
		ReduceNum:     nReduce,
		MapAllDone:    false,
		ReduceALLDone: false,
	}
	if nReduce > len(files) {
		m.Taskchans = make(chan WorkerTask, nReduce)
	} else {
		m.Taskchans = make(chan WorkerTask, len(files))
	}
	// Your code here.
	m.server()
	return &m
}

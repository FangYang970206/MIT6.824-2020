package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Flag struct {
	processing bool
	finished   bool
	startTime  time.Time
}

type Master struct {
	// Your definitions here.
	FileNames     []string
	MapFlags      []Flag
	ReduceFlags   []Flag
	MapTaskCnt    []int
	ReduceTaskCnt []int
	MapAllDone    bool
	ReduceALLDone bool
	MapNum        int
	ReduceNum     int
	Mut           sync.Mutex
	//Taskchans     chan WorkerTask
}

func (m *Master) HandlerTimeOut() {
	for {
		m.Mut.Lock()
		if m.MapAllDone && m.ReduceALLDone {
			m.Mut.Unlock()
			break
		}
		time.Sleep(30 * time.Millisecond)
		if !m.MapAllDone {
			for idx := 0; idx < m.MapNum; idx++ {
				if m.MapFlags[idx].finished == false {
					fmt.Printf("Map ID : %d clear zero\n", idx)
					m.MapFlags[idx].processing = false
					// timeNow := time.Now()
					// if timeNow.Sub(m.MapFlags[idx].startTime) > 1200*time.Millisecond {
					// 	m.MapFlags[idx].processing = false
					// }
				}
			}
		} else {
			for idx := 0; idx < m.ReduceNum; idx++ {
				if m.ReduceFlags[idx].finished == false {
					fmt.Printf("Reduce ID : %d clear zero\n", idx)
					m.ReduceFlags[idx].processing = false
					// timeNow := time.Now()
					// if timeNow.Sub(m.ReduceFlags[idx].startTime) > 1200*time.Millisecond {
					// 	m.ReduceFlags[idx].processing = false
					// }
				}
			}
		}
		m.Mut.Unlock()
		time.Sleep(2000 * time.Millisecond)
	}
}

func (m *Master) print() {
	for {
		m.Mut.Lock()
		for idx := 0; idx < m.MapNum; idx++ {
			fmt.Printf("MapID processing: %v, finished: %v\n", m.MapFlags[idx].processing, m.MapFlags[idx].finished)
		}
		for idx := 0; idx < m.ReduceNum; idx++ {
			fmt.Printf("ReduceId processing: %v, finished: %v\n", m.ReduceFlags[idx].processing, m.ReduceFlags[idx].finished)
		}
		fmt.Printf("Map alldone: %v\n", m.MapAllDone)
		fmt.Printf("Reduce alldone: %v\n", m.ReduceALLDone)
		m.Mut.Unlock()
		time.Sleep(time.Second * 10)
	}
}

func (m *Master) CreateWorkerTask(args *CreateWorkerArgs, workerTask *WorkerTask) error {
	m.Mut.Lock()
	defer m.Mut.Unlock()
	if !m.MapAllDone {
		for idx := 0; idx < m.MapNum; idx++ {
			if !m.MapFlags[idx].processing && !m.MapFlags[idx].finished {
				workerTask.ReduceNum = m.ReduceNum
				workerTask.MapNum = m.MapNum
				workerTask.State = MapState
				workerTask.MapID = idx
				workerTask.FileName = m.FileNames[idx]
				m.MapTaskCnt[idx]++
				workerTask.MapTaskCnt = m.MapTaskCnt[idx]
				m.MapFlags[idx].processing = true
				m.MapFlags[idx].startTime = time.Now()
				fmt.Printf("dispatch Map task ID: %d, MapTaskCnt: %d\n", idx, m.MapTaskCnt[idx])
				fmt.Printf("Map task State: %d\n", workerTask.State)
				return nil
			}
		}
		workerTask.State = WaitState
		return nil
	}
	if !m.ReduceALLDone {
		for idx := 0; idx < m.ReduceNum; idx++ {
			if !m.ReduceFlags[idx].processing && !m.ReduceFlags[idx].finished {
				workerTask.State = ReduceState
				workerTask.ReduceNum = m.ReduceNum
				workerTask.MapNum = m.MapNum
				workerTask.ReduceID = idx
				m.ReduceTaskCnt[idx]++
				workerTask.ReduceTaskCnt = m.ReduceTaskCnt[idx]
				m.ReduceFlags[idx].processing = true
				m.ReduceFlags[idx].startTime = time.Now()
				fmt.Printf("dispatch Reduce task ID: %d, ReduceTaskCnt: %d\n", idx, m.ReduceTaskCnt[idx])
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
			fmt.Printf("Map task ID : %d success and MapTaskCnt %d Expect MapTaskCnt %d\n", wr.MapID, wr.MapTaskCnt, m.MapTaskCnt[wr.MapID])
			if wr.MapTaskCnt == m.MapTaskCnt[wr.MapID] {
				fmt.Printf("Map task ID : %d finish and MapTaskCnt %d\n", wr.MapID, wr.MapTaskCnt)
				m.MapFlags[wr.MapID].finished = true
				m.MapFlags[wr.MapID].processing = false
			}
		} else {
			fmt.Printf("Reduce task ID : %d finish and ReduceTaskCnt %d Expect ReduceTaskCnt %d\n", wr.ReduceID, wr.ReduceTaskCnt, m.ReduceTaskCnt[wr.ReduceID])
			if wr.ReduceTaskCnt == m.ReduceTaskCnt[wr.ReduceID] {
				fmt.Printf("Reduce task ID : %d finish and ReduceTaskCnt %d\n", wr.ReduceID, wr.ReduceTaskCnt)
				m.ReduceFlags[wr.ReduceID].finished = true
				m.ReduceFlags[wr.ReduceID].processing = false
			}
		}
	} else {
		if wr.State == MapState {
			fmt.Printf("Map task ID : %d Fail and MapTaskCnt %d\n", wr.MapID, wr.MapTaskCnt)
			if m.MapFlags[wr.MapID].finished == false {
				m.MapFlags[wr.MapID].processing = false
			}
		} else {
			fmt.Printf("Reduce task ID : %d Fail and ReduceTaskCnt %d\n", wr.ReduceID, wr.ReduceTaskCnt)
			if m.ReduceFlags[wr.ReduceID].finished == false {
				m.ReduceFlags[wr.ReduceID].processing = false
			}
		}
	}
	for id := 0; id < m.MapNum; id++ {
		if !m.MapFlags[id].finished {
			break
		} else {
			if id == m.MapNum-1 {
				m.MapAllDone = true
				time.Sleep(500 * time.Millisecond)
			}
		}
	}
	for id := 0; id < m.ReduceNum; id++ {
		if !m.ReduceFlags[id].finished {
			break
		} else {
			if id == m.ReduceNum-1 {
				m.ReduceALLDone = true
			}
		}
	}
	return nil
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
		MapTaskCnt:    make([]int, len(files)),
		ReduceTaskCnt: make([]int, nReduce),
	}
	//if nReduce > len(files) {
	//	m.Taskchans = make(chan WorkerTask, nReduce)
	//} else {
	//	m.Taskchans = make(chan WorkerTask, len(files))
	//}
	// Your code here.
	m.server()
	go m.print()
	go m.HandlerTimeOut()
	return &m
}

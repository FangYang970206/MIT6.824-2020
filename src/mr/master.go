package mr

import (
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
}

type Master struct {
	FileNames      []string
	MapFlags       []Flag
	ReduceFlags    []Flag
	MapTaskCnts    []int
	ReduceTaskCnts []int
	MapAllDone     bool
	ReduceALLDone  bool
	MapNum         int
	ReduceNum      int
	Mut            sync.Mutex
}

func (m *Master) HandleTimeOut(args *NoArgs, reply *NoReply) error {
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
					m.MapFlags[idx].processing = false
				}
			}
		} else {
			for idx := 0; idx < m.ReduceNum; idx++ {
				if m.ReduceFlags[idx].finished == false {
					m.ReduceFlags[idx].processing = false
				}
			}
		}
		m.Mut.Unlock()
		time.Sleep(2000 * time.Millisecond)
	}
	return nil
}

func (m *Master) CreateWorkerTask(args *NoArgs, workerTask *WorkerTask) error {
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
				m.MapTaskCnts[idx]++
				workerTask.MapTaskCnt = m.MapTaskCnts[idx]
				m.MapFlags[idx].processing = true
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
				m.ReduceTaskCnts[idx]++
				workerTask.ReduceTaskCnt = m.ReduceTaskCnts[idx]
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

func (m *Master) HandleWorkerReport(wr *WorkerReportArgs, task *NoReply) error {
	m.Mut.Lock()
	defer m.Mut.Unlock()
	if wr.IsSuccess {
		if wr.State == MapState {
			if wr.MapTaskCnt == m.MapTaskCnts[wr.MapID] {
				m.MapFlags[wr.MapID].finished = true
				m.MapFlags[wr.MapID].processing = false
			}
		} else {
			if wr.ReduceTaskCnt == m.ReduceTaskCnts[wr.ReduceID] {
				m.ReduceFlags[wr.ReduceID].finished = true
				m.ReduceFlags[wr.ReduceID].processing = false
			}
		}
	} else {
		if wr.State == MapState {
			if m.MapFlags[wr.MapID].finished == false {
				m.MapFlags[wr.MapID].processing = false
			}
		} else {
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
func (m *Master) Done(args *NoArgs, reply *MasterDoneReply) error {
	reply.Done = m.MapAllDone && m.ReduceALLDone
	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{FileNames: files,
		MapFlags:       make([]Flag, len(files), len(files)),
		ReduceFlags:    make([]Flag, nReduce, nReduce),
		MapNum:         len(files),
		ReduceNum:      nReduce,
		MapAllDone:     false,
		ReduceALLDone:  false,
		MapTaskCnts:    make([]int, len(files)),
		ReduceTaskCnts: make([]int, nReduce),
	}
	m.server()
	args, reply := NoArgs{}, NoReply{}
	go m.HandleTimeOut(&args, &reply)
	return &m
}

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

//import "time"

type Master struct {
	// Your definitions here.
	files      []string
	filenames  []string
	processing []bool
	fileToPID  map[int]int  //字符串文件分配到那个PID进程
	finished   map[int]bool //PID进程是否完成了任务
	fileNumber int
	mut        sync.Mutex
}

func (m *Master) mapResult(arg *mapResultArgs, reply *mapResultReply) error {
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) HandlerWorker(arg *Args, reply *Reply) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	for i := 0; i < m.fileNumber; i++ {
		if !m.finished[i] && !m.processing[i] {
			m.processing[i] = true
			m.fileToPID[i] = arg.Pid
			reply.Content = m.files[i]
			reply.Filename = m.filenames[i]
			return nil
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{processing: make([]bool, len(files)),
		fileToPID:  make(map[int]int),
		finished:   make(map[int]bool),
		fileNumber: len(files)}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		m.files = append(m.files, string(content))
		m.filenames = append(m.filenames, filename)
	}
	// Your code here.

	m.server()
	return &m
}

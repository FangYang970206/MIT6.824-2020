package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (wt *WorkerTask) GetWorkerTask() {
	cwa := CreateWorkerArgs{}
	call("Master.CreateWorkerTask", &cwa, wt)
}

func (wt *WorkerTask) ReportWorkerTask(err error) {
	wra := WorkerReportArgs{
		MapID:     wt.MapID,
		ReduceID:  wt.ReduceID,
		State:     wt.State,
		IsSuccess: true,
	}
	wrr := WorkerReportReply{}
	if err != nil {
		wra.IsSuccess = false
	}
	call("Master.HandlerWorkerReport", &wra, &wrr)
}

func (wt *WorkerTask) DoMapWork() {
	file, err := os.Open(wt.FileName)
	if err != nil {
		wt.ReportWorkerTask(err)
		log.Fatalf("cannot open %v", wt.FileName)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		wt.ReportWorkerTask(err)
		log.Fatalf("cannot read %v", wt.FileName)
		return
	}
	file.Close()
	kvs := wt.MapFunction(wt.FileName, string(content))
	intermediate := make([][]KeyValue, wt.ReduceNum, wt.ReduceNum)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % wt.ReduceNum
		intermediate[idx] = append(intermediate[idx], kv)
	}
	for idx := 0; idx < wt.ReduceNum; idx++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", wt.MapID, idx)
		file, err = os.Create(intermediateFileName)
		if err != nil {
			wt.ReportWorkerTask(err)
			log.Fatalf("cannot create %v", intermediateFileName)
			return
		}
		data, _ := json.Marshal(intermediate[idx])
		_, err = file.Write(data)
		if err != nil {
			wt.ReportWorkerTask(err)
			log.Fatalf("connot write file: %v", intermediateFileName)
			return
		}
	}
	wt.ReportWorkerTask(nil)
}

func (wt *WorkerTask) DoReduceWork() {

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	wt := WorkerTask{MapFunction: mapf,
		ReduceFunction: reducef}
	for {
		wt.GetWorkerTask()
		if wt.State == MapState {
			wt.DoMapWork()
		} else if wt.State == ReduceState {
			wt.DoReduceWork()
		} else if wt.State == StopState {
			break
		} else if wt.State == WaitState {
			time.Sleep(10 * time.Millisecond)
		}
	}
	return
	// uncomment to send the Example RPC to the master.
	//CallExample()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	call("Master.Example", &args, &reply)
//
//	// reply.Y should be 100.
//	fmt.Printf("reply.Y %v\n", reply.Y)
//}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

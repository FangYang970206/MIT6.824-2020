package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

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
	cwa := NoArgs{}
	newWt := WorkerTask{}
	call("Master.CreateWorkerTask", &cwa, &newWt)
	if newWt.State == MapState {
		wt.ReduceNum = newWt.ReduceNum
		wt.MapNum = newWt.MapNum
		wt.State = newWt.State
		wt.MapID = newWt.MapID
		wt.FileName = newWt.FileName
		wt.MapTaskCnt = newWt.MapTaskCnt
	} else if newWt.State == ReduceState {
		wt.State = newWt.State
		wt.ReduceID = newWt.ReduceID
		wt.ReduceTaskCnt = newWt.ReduceTaskCnt
		wt.MapNum = newWt.MapNum
		wt.ReduceNum = newWt.ReduceNum
	} else if newWt.State == StopState {
		wt.State = newWt.State
	} else {
		wt.State = newWt.State
	}
}

func (wt *WorkerTask) ReportWorkerTask(err error) {
	wra := WorkerReportArgs{
		MapID:     wt.MapID,
		ReduceID:  wt.ReduceID,
		State:     wt.State,
		IsSuccess: true,
	}
	if wt.State == MapState {
		wra.MapTaskCnt = wt.MapTaskCnt
	} else {
		wra.ReduceTaskCnt = wt.ReduceTaskCnt
	}
	wrr := NoReply{}
	if err != nil {
		wra.IsSuccess = false
	}
	call("Master.HandleWorkerReport", &wra, &wrr)
}

func (wt *WorkerTask) DoMapWork() {
	file, err := os.Open(wt.FileName)
	if err != nil {
		wt.ReportWorkerTask(err)
		fmt.Printf("cannot open %v", wt.FileName)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		wt.ReportWorkerTask(err)
		fmt.Printf("cannot read %v", wt.FileName)
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
			fmt.Printf("cannot create %v", intermediateFileName)
			return
		}
		data, _ := json.Marshal(intermediate[idx])
		_, err = file.Write(data)
		if err != nil {
			wt.ReportWorkerTask(err)
			fmt.Printf("connot write file: %v", intermediateFileName)
			return
		}
		file.Close()
	}
	wt.ReportWorkerTask(nil)
}

func (wt *WorkerTask) DoReduceWork() {
	kvsReduce := make(map[string][]string)
	for idx := 0; idx < wt.MapNum; idx++ {
		filename := fmt.Sprintf("mr-%d-%d", idx, wt.ReduceID)
		file, err := os.Open(filename)
		if err != nil {
			wt.ReportWorkerTask(err)
			fmt.Printf("open file %v fail", filename)
			return
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			wt.ReportWorkerTask(err)
			fmt.Printf("read file %v fail", filename)
			return
		}
		file.Close()
		kvs := make([]KeyValue, 0)
		err = json.Unmarshal(content, &kvs)
		if err != nil {
			wt.ReportWorkerTask(err)
			fmt.Printf("json file %v fail", filename)
			return
		}
		for _, kv := range kvs {
			_, ok := kvsReduce[kv.Key]
			if !ok {
				kvsReduce[kv.Key] = make([]string, 0)
			}
			kvsReduce[kv.Key] = append(kvsReduce[kv.Key], kv.Value)
		}
	}
	ReduceResult := make([]string, 0)
	for key, val := range kvsReduce {
		ReduceResult = append(ReduceResult, fmt.Sprintf("%v %v\n", key, wt.ReduceFunction(key, val)))
	}
	outFileName := fmt.Sprintf("mr-out-%d", wt.ReduceID)
	err := ioutil.WriteFile(outFileName, []byte(strings.Join(ReduceResult, "")), 0644)
	if err != nil {
		wt.ReportWorkerTask(err)
		fmt.Printf("write %v fail:", outFileName)
		return
	}
	wt.ReportWorkerTask(nil)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	wt := WorkerTask{
		MapFunction:    mapf,
		ReduceFunction: reducef,
	}
	for {
		wt.GetWorkerTask()
		if wt.State == MapState {
			wt.DoMapWork()
		} else if wt.State == ReduceState {
			wt.DoReduceWork()
		} else if wt.State == StopState {
			break
		} else if wt.State == WaitState {
			time.Sleep(300 * time.Millisecond)
		}
	}
	return
}

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
	return false
}

package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type WorkerProcess struct {
	WorkId  int
	Mapf    func(string, string) []KeyValue
	Reducef func(string, []string) string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := WorkerProcess{
		Mapf:    mapf,
		Reducef: reducef,
	}
	worker.register()
	worker.run()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
func (w *WorkerProcess) run() {
	//log.Printf("worker start run!\n")
	for {
		task := w.getTask()
		if task.Stage == TaskStage_Map {
			w.doMap(*task)
		} else if task.Stage == TaskStage_Reduce {
			w.doReduce(*task)
		} else {
			//log.Printf("wrong task-%+v\n", task)
			continue
		}
	}
}
func (w *WorkerProcess) MapFilename(index int, pid int) string {
	return fmt.Sprintf("mr-kv-%d-%d", index, pid)
}
func (w *WorkerProcess) ReduceFilename(pid int) string {
	return fmt.Sprintf("mr-out-%d", pid)
}
func (w *WorkerProcess) doMap(task Task) {
	//log.Printf("start do map task")
	file, err := os.Open(task.Filename)
	if err != nil {
		w.report(task, false)
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		w.report(task, false)
		log.Fatalf("cannot read %v", task.Filename)
	}
	err = file.Close()
	if err != nil {
		w.report(task, false)
		log.Fatalf("cannot close %v", task.Filename)
	}
	kvs := w.Mapf(task.Filename, string(content))
	partitions := make([][]KeyValue, task.NReduce)

	for _, kv := range kvs {
		pid := ihash(kv.Key) % task.NReduce
		partitions[pid] = append(partitions[pid], kv)
	}
	//k is the first dim'index, v is an []keyvalue
	for k, v := range partitions {
		oname := w.MapFilename(task.Id, k)
		ofile, _ := os.Create(oname)
		encoder := json.NewEncoder(ofile)
		for _, kv := range v {
			if err := encoder.Encode(&kv); err != nil {
				w.report(task, false)
				//log.Printf("encode  kvs to file-%v  fail in doMapTask. %v", oname, err)
			}
		}

		err := ofile.Close()
		if err != nil {
			w.report(task, false)
			log.Fatalf("cannot close %v", task.Filename)
		}

	}
	w.report(task, true)

}
func (w *WorkerProcess) doReduce(task Task) {
	//log.Printf("start do Reduce task %+v\n", task)

	maps := make(map[string][]string)
	for i := 0; i < task.NMap; i++ {
		filename := w.MapFilename(i, task.Id)
		file, err := os.Open(filename)
		if err != nil {
			w.report(task, false)
			//log.Fatalf("cannot open %v\n", filename)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	res := make([]string, 0)
	for k, v := range maps {
		len := w.Reducef(k, v)
		res = append(res, fmt.Sprintf("%v %v\n", k, len))
	}

	fileName := w.ReduceFilename(task.Id)
	if err := ioutil.WriteFile(fileName, []byte(strings.Join(res, "")), 0600); err != nil {
		//log.Printf("write file-%v in doReduceTask. %v", fileName, err)
		w.report(task, false)
	}
	w.report(task, true)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
func (w *WorkerProcess) register() {
	args := RegArgs{}
	reply := RegReply{}
	ok := call("Coordinator.Register", &args, &reply)
	if ok {
		//log.Printf("worker register id %v\n", reply.WorkerId)
		w.WorkId = reply.WorkerId
	} else {
		log.Fatalf("register failed!\n")
	}
}
func (w *WorkerProcess) getTask() *Task {
	args := AssignArgs{
		WorkerId: w.WorkId,
	}
	reply := AssignReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		//log.Printf("worker-%v get a task%+v\n", w.WorkId, reply.Task)
	} else {
		log.Fatalf("get task failed!\n")
		return nil
	}
	return reply.Task
}
func (w *WorkerProcess) report(task Task, done bool) {
	args := ReportArgs{
		Stage:    task.Stage,
		TaskId:   task.Id,
		WorkerId: w.WorkId,
		Done:     done,
	}
	reply := ReportReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		//log.Printf("task report success!\n")
	} else {
		//log.Printf("task report failed\n")
	}
}

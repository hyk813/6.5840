package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	ScheduleInterval   = time.Millisecond * 500 //扫描任务状态的间隔时间
	MaxTaskRunningTime = time.Second * 5        //每个任务的最大执行时间，用于判断是否超时
)
const (
	TaskStage_Map    int = 0
	TaskStage_Reduce int = 1
)
const (
	TaskStatus_new      int = 0
	TaskStatus_ready    int = 1
	TaskStatus_running  int = 2
	TaskStatus_finished int = 3
	TaskStatus_failed   int = 4
)

type Task struct {
	Stage    int
	Filename string
	Id       int
	NMap     int
	NReduce  int
}
type TaskState struct {
	WorkId    int
	StartTime time.Time
	Status    int
}

type Coordinator struct {
	// Your definitions here.
	files     []string
	nReduce   int
	nMap      int
	Stage     int
	Tasks     []TaskState
	TaskQueue chan Task
	mulock    sync.Mutex
	workerNum int
	done      bool
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) Register(args *RegArgs, reply *RegReply) error {
	c.mulock.Lock()
	defer c.mulock.Unlock()
	c.workerNum++
	reply.WorkerId = c.workerNum
	DPrintf("register woker:%v", c.workerNum)
	return nil
}
func (c *Coordinator) AssignTask(args *AssignArgs, reply *AssignReply) error {
	task := <-c.TaskQueue
	reply.Task = &task
	c.mulock.Lock()
	defer c.mulock.Unlock()
	c.Tasks[task.Id].WorkId = args.WorkerId
	c.Tasks[task.Id].StartTime = time.Now()
	c.Tasks[task.Id].Status = TaskStatus_running
	DPrintf("Assign Task to worker:%v,Task:%+v", c.workerNum, task)
	return nil
}
func (c *Coordinator) ReportTask(args *ReportArgs, reply *ReportReply) error {
	c.mulock.Lock()
	defer c.mulock.Unlock()
	id := args.TaskId
	if args.Stage != c.Stage || args.WorkerId != c.Tasks[id].WorkId {
		return nil
	}

	if args.Done {
		c.Tasks[id].Status = TaskStatus_finished
		DPrintf("TASK finished:%+v", args)
	} else {
		c.Tasks[id].Status = TaskStatus_failed
		DPrintf("TASK failed:%+v", args)
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
func (c *Coordinator) run() {
	for !c.Done() {
		c.scanTaskState()
		time.Sleep(ScheduleInterval)
	}
}
func (c *Coordinator) InitTask(id int) Task { //进入持锁
	task := Task{
		Stage:    c.Stage,
		Id:       id,
		Filename: "",
		NMap:     c.nMap,
		NReduce:  c.nReduce,
	}
	if c.Stage == TaskStage_Map {
		task.Filename = c.files[id]
	}
	return task
}
func (c *Coordinator) scanTaskState() {
	c.mulock.Lock()
	defer c.mulock.Unlock()
	alldone := true
	for k, v := range c.Tasks {
		switch v.Status {
		case TaskStatus_new:
			alldone = false
			c.Tasks[k].Status = TaskStatus_ready
			c.TaskQueue <- c.InitTask(k)
		case TaskStatus_ready:
			alldone = false
		case TaskStatus_running:
			alldone = false
			if time.Now().Sub(v.StartTime) > MaxTaskRunningTime {
				c.Tasks[k].Status = TaskStatus_ready
				c.TaskQueue <- c.InitTask(k)
			}
		case TaskStatus_finished:
		case TaskStatus_failed:
			alldone = false
			c.Tasks[k].Status = TaskStatus_ready
			c.TaskQueue <- c.InitTask(k)
		}
	}
	if alldone == true {
		if c.Stage == TaskStage_Map {
			c.Stage = TaskStage_Reduce
			c.Tasks = make([]TaskState, c.nReduce) //重置tasks数组
			DPrintf("Change to Reduce")
		} else {
			c.done = true
		}
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mulock.Lock()
	defer c.mulock.Unlock()
	ret = c.done
	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:     files,
		nReduce:   nReduce,
		nMap:      len(files),
		Stage:     TaskStage_Map,
		workerNum: 0,
		done:      false,
		Tasks:     make([]TaskState, len(files)),
	}
	if nReduce > len(files) {
		c.TaskQueue = make(chan Task, nReduce)
	} else {
		c.TaskQueue = make(chan Task, len(files))
	}

	// Your code here.

	go c.run()
	c.server()
	return &c
}

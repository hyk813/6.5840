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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RegArgs struct {
	//不需要参数
}
type RegReply struct {
	WorkerId int
}
type AssignArgs struct {
	WorkerId int
}
type AssignReply struct {
	Task *Task
}
type ReportArgs struct {
	WorkerId int
	TaskId   int
	Stage    int
	Done     bool
	//由哪个worker执行，因为报告具有延迟性，可能该节点已经过时严重；是哪个task，由stage和taskid共同决定；执行情况；
}
type ReportReply struct {
	//不需要恢复
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

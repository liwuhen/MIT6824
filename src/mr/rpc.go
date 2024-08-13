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

type TaskStatus int 
const (
	Finshed = iota
	Failed 
	Unassigned
	Assigned
)

type TaskResponse struct {
	Tasktype State // 0 map  1 reduce 2 Wait 3 Done
	Filename string
	NReduce  int
	TaskId   int
	Intermediatesfile []string // 每个分区存储的所有map阶段产生中间文件
}

type TaskResquest struct {
	TaskId       int
	TaskType     State
	TaskState    TaskStatus
	Intermediate []string
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

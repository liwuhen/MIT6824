package mr

import "log"
import "net"
import "os"
import "sync"
// import "fmt"
import "time"
import "strconv"
import "strings"
import "net/rpc"
import "net/http"

type State int 
const (
	Map = iota
	Reduce 
	Wait
	Done
)

type Task struct {
	Filename       string   // Map 任务的输入文件
	NReduce        int		// NReduce 任务的个数
	TaskId         int      // 任务 ID
	TaskType       State    // 任务类型
	TaskState      TaskStatus   // 任务状态
	TaskTime       time.Time    // 任务派发时间
	Intermediatesfile  []string // 每个分区存储的所有 map 阶段产生中间文件
}

type TaskMasterState struct {
	TaskState    TaskStatus // 任务执行状态
	TaskTime     time.Time  // 任务派发时间
	ReassignTask *Task   // 任务失败时重新派发
}

type Coordinator struct {
	// Your definitions here.
	TaskQueue     chan *Task  // 任务队列
	TaskInfo      map[int] *TaskMasterState
	MasterState   State  // Master 的任务派发状态
	NumMap        int    // Map 数量
	NReduce       int    // Reduce 数量
	Intermediates map[int][]string // Map 任务产生的 R 个中间文件的信息
	Mutex         sync.Mutex
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{TaskQueue: make(chan *Task, max(nReduce, len(files))),
		 TaskInfo: make(map[int] *TaskMasterState),
		 MasterState: Map,
		 NumMap: len(files),
		 NReduce: nReduce,
		 Intermediates: make(map[int][]string),
		 Mutex: sync.Mutex{}}

	// 制造 map 任务
	c.MakeMapTask(files)

	c.server()  // 启动 RPC 服务器

	// 利用单独协程循环检测任务状态
	c.CheckTaskTime()
	return &c
}

// Your code here -- RPC handlers for the worker to call.
// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetTask(args *TaskResquest, reply *TaskResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if len(c.TaskQueue) > 0 {
		task := *<-c.TaskQueue
		task.TaskTime = time.Now()
		if task.TaskState == Unassigned || task.TaskState == Failed ||  
		   (task.TaskState == Assigned && time.Since(task.TaskTime) > 10*time.Second){

			// fmt.Println("c.MasterState: ", c.MasterState, "task.TaskState: ",task.TaskState)
			// fmt.Println("len(c.TaskQueue): ", len(c.TaskQueue), "c.MasterState: ",c.MasterState,"task.TaskId: ",task.TaskId)
			switch c.MasterState {
			case Map:
				reply.Tasktype = task.TaskType
				reply.Filename = task.Filename
				reply.NReduce  = task.NReduce
				reply.TaskId   = task.TaskId
				// fmt.Println("reply.Tasktype: ", reply.Tasktype, "reply.Filename: ",reply.Filename)
			case Reduce:
				reply.Tasktype = task.TaskType
				reply.NReduce  = task.NReduce
				reply.TaskId   = task.TaskId
				reply.Intermediatesfile = task.Intermediatesfile
			}
			c.TaskInfo[task.TaskId].TaskState = Assigned
			c.TaskInfo[task.TaskId].TaskTime  = time.Now()
		}
	} else if c.MasterState == Done {
		reply.Tasktype = Done
	} else {
		reply.Tasktype = Wait
	}

	return nil
}

func (c *Coordinator) GetTaskResponse(args *TaskResquest, reply *TaskResponse) error {

	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	// master 收集所有 map 阶段 worker 服务器缓存中间文件的位置信息
	c.Intermediates[args.TaskId] = append(c.Intermediates[args.TaskId], args.Intermediate...)

	// worker 任务状态的信息回传，master 任务状态的更新
	c.TaskInfo[args.TaskId].TaskState = args.TaskState
	
	// 检测所有阶段 task 是否完成
	if args.TaskType == Map {
		// 所有 map 任务完成
		if c.CheckAllTask() {
			// 进行 Reduce 任务
			c.MakeReduceTask()
			c.MasterState = Reduce
		}
	} 
	
	if args.TaskType == Reduce {
		if c.CheckAllTask() {
			c.MasterState = Done
		}
	}
	
	return nil
}

func (c *Coordinator) MakeMapTask(files []string) {
	for id, file := range files { 
		task := &Task {Filename: file, 
					   NReduce: c.NReduce,
					   TaskId: id,
					   TaskType: Map,
					   TaskState: Unassigned,
					   Intermediatesfile: make([]string, 0)}
		c.TaskQueue <- task
		c.TaskInfo[id] = &TaskMasterState {TaskState: Unassigned,
										   ReassignTask: task}
	}
}

func (c *Coordinator) MakeReduceTask() {
	Intermediatefile := c.CreateFile()
	for id := 0; id < c.NReduce; id++ { 

		task := &Task {Filename: "", 
					   NReduce: c.NReduce,
					   TaskId: id,
					   TaskType: Reduce,
					   TaskState: Unassigned,
					   Intermediatesfile: Intermediatefile[id]}
		c.TaskQueue <- task
		c.TaskInfo[id] = &TaskMasterState {TaskState: Unassigned,
										   ReassignTask: task}
	}
}

func (c *Coordinator) CreateFile() map[int][]string {
	// fmt.Println("----->c.Intermediates: ", c.Intermediates)
	// 按照分区提前所有 map 阶段 worker 的中间文件
	Intermediatefile := make(map[int][]string)
	for id := 0; id < c.NReduce; id++ { 
		temp_file := make([]string, 0)
		for _, values := range c.Intermediates {
			for _, value := range values {
				inter_file_str := strings.Split(value, "/")
				inter_file  := inter_file_str[len(inter_file_str)-1]
				zoning_id,_ := strconv.Atoi(strings.Split(inter_file, "-")[len(strings.Split(inter_file, "-"))-1])
				if zoning_id == id {
					temp_file = append(temp_file, value)
					break
				}
			}
		}
		Intermediatefile[id] = temp_file
	}
	// fmt.Println("----->Intermediatefile: ", Intermediatefile)
	return Intermediatefile
}

func (c *Coordinator) CheckAllTask() bool {

	for _, taskinfo := range c.TaskInfo {
		if taskinfo.TaskState != Finshed {
			return false
		}
	}
	return true
}

func (c *Coordinator) CheckTaskTime() {
	
	for {
		time.Sleep(5 * time.Second)

		c.Mutex.Lock()
		// 判断协程退出状态
		if c.MasterState == Done {
			c.Mutex.Unlock()
			return
		}

		for _, TaskMasterState := range c.TaskInfo {
			if TaskMasterState.TaskState == Assigned && 
				time.Since(TaskMasterState.TaskTime) > 10*time.Second {
					TaskMasterState.TaskState = Failed
					c.TaskQueue <- TaskMasterState.ReassignTask
			}
		}
		c.Mutex.Unlock()
	}
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	ret := false
	if c.MasterState == Done {
		ret = true
		time.Sleep(1*time.Second)
	}
	return ret
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}


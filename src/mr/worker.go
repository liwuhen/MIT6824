package mr

import "os"
import "fmt"
import "log"
import "time"
import "sort"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "path/filepath"
import "encoding/json"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// uncomment to send the Example RPC to the coordinator.
	// 启动worker进行轮盘查询
	for {
		task := CallGetTask()
		switch task.Tasktype {
			case Map:
				DoMapTask(&task, mapf)
			case Reduce:
				DoReduceTask(&task, reducef)
			case Wait:
				time.Sleep(5 * time.Second)
			case Done:
				return
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallGetTask() TaskResponse{
	args := TaskResquest{}
	reply:= TaskResponse{}
	// send the RPC request, wait for the reply.
	call("Coordinator.GetTask", &args, &reply)
	// if ok := call("Coordinator.GetTask", &args, &reply); ok {
	// 	fmt.Println("Reply Task : ", reply.Tasktype, "id :", reply.TaskId)
	// } else {
	// 	fmt.Println("Call Back Failed!")
	// }

	return reply
}

func DoMapTask(task *TaskResponse, mapf func(string, string) []KeyValue) {
	intermediates := []KeyValue{}
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("Map Task cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Map Task cannot read %v", task.Filename)
	}
	file.Close()
	kva := mapf(task.Filename, string(content))
	intermediates = append(intermediates, kva...)

	//缓存后的结果会写到本地磁盘，并切成R份
	//切分方式是根据key做hash
	buffer := make([][]KeyValue, task.NReduce)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReduce
		buffer[slot] = append(buffer[slot], intermediate)
	}

	out_map_file_path := make([]string, 0)
	for index := 0; index < task.NReduce; index++ {
		filepath := WriteTempFile(task.TaskId, index, &buffer[index])
		out_map_file_path = append(out_map_file_path, filepath)
	}
	// worker回传中间文件的位置信息
	// send the RPC request. 更新每个worker在map阶段的任务状态
	args := TaskResquest {
		TaskId: task.TaskId,
		TaskState: Finshed,
		TaskType: Map,
	    Intermediate: out_map_file_path}

	call("Coordinator.GetTaskResponse", &args, &TaskResponse{})
	// if ok := call("Coordinator.GetTaskResponse", &args, &TaskResponse{}); ok {
	// 	fmt.Println("Update Map Task State")
	// } else {
	// 	fmt.Println("Update Map Task State Failed!")
	// }
	
}

func DoReduceTask(task *TaskResponse, reducef func(string, []string) string) {
	intermediates := *ReadTempFile(task)
	sort.Sort(ByKey(intermediates))
	dir, _ := os.Getwd()
	outFile, err := ioutil.TempFile(dir, "mr-temp-*")
	if err != nil {
		log.Fatal("Failed to create reduce phase out file", err)
	}
	
	// 聚合
	for index := 0; index < len(intermediates); {
		index_j := index + 1
		for index_j < len(intermediates) && intermediates[index_j].Key == intermediates[index].Key {
			index_j++
		}

		values := []string{}
		for index_i := index; index_i < index_j; index_i++ {
			values = append(values, intermediates[index_i].Value)
		}
		
		output := reducef(intermediates[index].Key, values)
		//写到对应的output文件
		fmt.Fprintf(outFile, "%v %v\n", intermediates[index].Key, output)
		index = index_j
	}
	outFile.Close()
	outputName := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(outFile.Name(), outputName)

	// worker回传Reduce 阶段的任务状态
	// send the RPC request. 更新每个worker在Reduce阶段的任务状态
	args := TaskResquest {
		TaskId: task.TaskId,
		TaskState: Finshed,
		TaskType: Reduce}

	call("Coordinator.GetTaskResponse", &args, &TaskResponse{})
	// if ok := call("Coordinator.GetTaskResponse", &args, &TaskResponse{}); ok {
	// 	fmt.Println("Update Reduce Task State")
	// } else {
	// 	fmt.Println("Update Reduce Task State Failed!")
	// }
}

func WriteTempFile(x int, y int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), outputName)
	return filepath.Join(dir, outputName)
}

func ReadTempFile(task *TaskResponse) *[]KeyValue{
	intermediates := []KeyValue{}
	for _, filepath := range task.Intermediatesfile {
		file, err :=os.Open(filepath)
		if err != nil {
			log.Fatalf("cannot open %v", filepath)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
			  break
			}
			intermediates = append(intermediates, kv)
		}
		file.Close()
	}
	return &intermediates
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
)

// 获取任务
func GetTask() *Task {
	args := &TaskArgs{}
	reply := &Task{}

	if ok := call("Coordinator.PullTask", args, reply); ok {
		fmt.Println("GetTask success! -- ", reply.TaskId, reply.TaskType)
	} else {
		fmt.Println("GetTask failed!")
	}

	return reply
}

// 标记当前任务完成
func MarkDone(task *Task) {
	reply := &Task{}
	if ok := call("Coordinator.TaskIsDone", task, reply); ok {
		fmt.Println("MarkDone success! -- ", task.TaskId, task.TaskType)
	} else {
		fmt.Println("MarkDone failed!")
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	alive := true
	for alive {
		task := GetTask()
		switch task.TaskType {
		case MapType:
			{
				MapTask(task, mapf)
				MarkDone(task)
			}
		case ReduceType:
			{
				ReduceTask(task, reducef)
				MarkDone(task)
			}
		case WaitType:
			{
				fmt.Println("Wait for task...")
				time.Sleep(time.Second)
			}
		case KillType:
			{
				time.Sleep(time.Second)
				fmt.Println("[Status]:", task.TaskId, "is killed!")
				alive = false
			}
		}
		time.Sleep(time.Second)
	}
}

// Map任务
func MapTask(task *Task, mapf func(string, string) []KeyValue) {
	fmt.Println("TaskFileName -- ", task.FilesName)
	filename := task.FilesName[0]
	// 打开文件
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	// 读取文件内容
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	reduceNum := task.NReduce
	intermediate := mapf(filename, string(content))
	HashKv := make([][]KeyValue, reduceNum)
	// 分发中间文件
	for _, v := range intermediate {
		index := ihash(v.Key) % reduceNum
		HashKv[index] = append(HashKv[index], v)
	}
	// 写入中间文件
	for i := 0; i < reduceNum; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		file, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("cannot create %v", fileName)
		}
		enc := json.NewEncoder(file)
		for _, kv := range HashKv[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		file.Close()
	}
}

// Reduce任务
func ReduceTask(task *Task, reducef func(string, []string) string) {
	reduceNum := task.TaskId
	fmt.Println("ReduceFilesName--", task.FilesName)
	intermediate := shuffle(task.FilesName)
	finalName := fmt.Sprintf("mr-out-%d", reduceNum)
	ofile, err := os.Create(finalName)
	if err != nil {
		log.Fatalf("cannot create %v", finalName)
	}
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
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

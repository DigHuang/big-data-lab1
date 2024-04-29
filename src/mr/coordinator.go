package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var mu sync.Mutex

// 拉取任务
func (c *Coordinator) PullTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	if c.CoCondition == MapCoor {
		if len(c.MapChan) > 0 {
			*reply = *<-c.MapChan
			if !c.taskMetaHolder.StartTask(reply.TaskId) {
				fmt.Println("PullTask Error Task", reply.TaskId, "Already Run")
			}
		} else {
			reply.TaskType = WaitType
			if c.taskMetaHolder.CheckTaskDone() {
				c.ToNextCoor()
			}
		}
	} else if c.CoCondition == ReduceCoor {
		if len(c.ReduceChan) > 0 {
			*reply = *<-c.ReduceChan
			if !c.taskMetaHolder.StartTask(reply.TaskId) {
				fmt.Println("PullTask Error Task", reply.TaskId, "Already Run")
			}
		} else {
			reply.TaskType = WaitType
			if c.taskMetaHolder.CheckTaskDone() {
				c.ToNextCoor()
			}
		}
	} else if c.CoCondition == AllDone {
		reply.TaskType = KillType
	}
	return nil
}

// 启动任务
func (j *TaskMetaHolder) StartTask(taskId int) bool {
	if task, ok := j.TaskMetaMap[taskId]; !ok || task.Condition != WaitTask {
		return false
	} else {
		task.Condition = WorkingTask
		task.StartTime = time.Now()
		return true
	}
}

// Coor 进入下一个状态
func (c *Coordinator) ToNextCoor() {
	if c.CoCondition == MapCoor {
		c.PushReduceTasks()
		c.CoCondition = ReduceCoor
		fmt.Println("Map Turn To Reduce")
	} else if c.CoCondition == ReduceCoor {
		c.CoCondition = AllDone
	}
}

// 标记任务完成
func (c *Coordinator) TaskIsDone(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapType:
		if meta, ok := c.taskMetaHolder.TaskMetaMap[args.TaskId]; ok && meta.Condition == WorkingTask {
			meta.Condition = DoneTask
			fmt.Println("Map Task Done -- ", args.TaskId)
		} else {
			fmt.Println("Map Task Already Done -- ", args.TaskId)
		}
	case ReduceType:
		if meta, ok := c.taskMetaHolder.TaskMetaMap[args.TaskId]; ok && meta.Condition == WorkingTask {
			meta.Condition = DoneTask
			fmt.Println("Reduce Task Done -- ", args.TaskId)
		} else {
			fmt.Println("Reduce Task Already Done -- ", args.TaskId)
		}
	default:
		panic("TaskType Error")
	}
	return nil
}

// 检查任务是否完成
func (j *TaskMetaHolder) CheckTaskDone() bool {
	for _, v := range j.TaskMetaMap {
		if v.Condition != DoneTask {
			return false
		}
	}
	return true
}

// 检查任务是否超时
func (c *Coordinator) CheckTaskTimeout() {
	for {
		time.Sleep(time.Second)
		mu.Lock()
		if c.CoCondition == AllDone {
			mu.Unlock()
			break
		}

		timeNow := time.Now()
		for _, v := range c.taskMetaHolder.TaskMetaMap {
			// 打印任务执行时间
			if v.Condition == WorkingTask {
				fmt.Println("Task Running -- ", v.TaskAddr.TaskId, timeNow.Sub(v.StartTime))
			}
			// 超时10s
			if v.Condition == WorkingTask && timeNow.Sub(v.StartTime) > time.Second*10 {
				fmt.Println("Task Timeout -- ", v.TaskAddr.TaskId)
				switch v.TaskAddr.TaskType {
				case MapType:
					{
						c.MapChan <- v.TaskAddr
						v.Condition = WaitTask
					}
				case ReduceType:
					{
						c.ReduceChan <- v.TaskAddr
						v.Condition = WaitTask
					}
				}
			}
		}
		mu.Unlock()
	}
}

// 检查任务是否可以放入
func (j *TaskMetaHolder) PushTasks(taskInfo *TaskMetaInfo) bool {
	taskId := taskInfo.TaskAddr.TaskId
	if _, ok := j.TaskMetaMap[taskId]; ok {
		fmt.Println("Task Already Exist -- ", taskId)
		return false
	} else {
		j.TaskMetaMap[taskId] = taskInfo
		return true
	}
}

// 将任务放入 MapChan
func (c *Coordinator) PushMapTasks(files []string) {
	for id, v := range files {
		task := Task{
			TaskType:  MapType,
			FilesName: []string{v},
			TaskId:    id,
			NReduce:   c.NReduce,
		}
		taskMetaInfo := TaskMetaInfo{
			Condition: WaitTask,
			TaskAddr:  &task,
		}
		c.taskMetaHolder.PushTasks(&taskMetaInfo)
		// 写入 MapChan
		c.MapChan <- &task
		fmt.Println("PushTasks MapChan Success --", v)
	}
	c.taskMetaHolder.CheckTaskDone()
}

// 将任务放入 ReduceChan
func (c *Coordinator) PushReduceTasks() {
	for i := 0; i < c.NReduce; i++ {
		id := i + len(c.Files)
		task := Task{
			TaskType:  ReduceType,
			FilesName: selectReduceFiles(i),
			TaskId:    id,
		}
		taskMetaInfo := TaskMetaInfo{
			Condition: WaitTask,
			TaskAddr:  &task,
		}
		c.taskMetaHolder.PushTasks(&taskMetaInfo)
		// 写入 ReduceChan
		c.ReduceChan <- &task
		fmt.Println("PushTasks ReduceChan Success --", i)
	}
	c.taskMetaHolder.CheckTaskDone()
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
	mu.Lock()
	defer mu.Unlock()
	ret := false
	ret = c.CoCondition == AllDone
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		CoCondition: MapCoor,
		MapChan:     make(chan *Task, len(files)),
		ReduceChan:  make(chan *Task, nReduce),
		NReduce:     nReduce,
		Files:       files,
		taskMetaHolder: TaskMetaHolder{
			TaskMetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}

	// Map任务
	c.PushMapTasks(files)

	// 启动 RPC 服务
	c.server()

	// 检查任务是否超时
	go c.CheckTaskTimeout()
	return &c
}

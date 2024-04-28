package mr

import "time"

type TaskArgs struct {
}

// Task Type
const (
	MapType    = "Map"
	ReduceType = "Reduce"
	WaitType   = "Wait"
	KillType   = "Kill"
)

// Task Condition
const (
	WaitTask    = "Wait"
	WorkingTask = "Working"
	DoneTask    = "Done"
)

// Coordinator Condition
const (
	MapCoor    = "Map"
	ReduceCoor = "Reduce"
	AllDone    = "AllDone"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

type Task struct {
	TaskType  string   // 任务类型：MapType/ReduceType/WaitType/KillType
	TaskId    int      // 任务ID
	FilesName []string // 文件名
	NReduce   int      // Reduce 任务数量
}

// 任务元数据
type TaskMetaInfo struct {
	Condition string    // 任务状态：WaitTask/WorkingTask/DoneTask
	StartTime time.Time // 任务开始时间
	TaskAddr  *Task     // 任务地址
}

// 保存全部任务元数据
type TaskMetaHolder struct {
	TaskMetaMap map[int]*TaskMetaInfo
}

type Coordinator struct {
	CoCondition    string         // Coor 状态: MapCoor/ReduceCoor/AllDone
	MapChan        chan *Task     // Map任务channel
	ReduceChan     chan *Task     // Reduce任务channel
	NReduce        int            // Reduce任务数量
	Files          []string       // 文件列表
	taskMetaHolder TaskMetaHolder // 任务元数据
}

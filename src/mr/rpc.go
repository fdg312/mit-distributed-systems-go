package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

const (
	TaskTypeMap    = 0
	TaskTypeReduce = 1
	TaskTypeWait   = 2
	TaskTypeExit   = 3
)

type AskTaskArgs struct{}

type AskTaskReply struct {
	TaskType int
	TaskID   int
	Filename string
	NReduce  int
	NMap     int
}

type ReportTaskArgs struct {
	TaskType int
	TaskID   int
}

type ReportTaskReply struct{}

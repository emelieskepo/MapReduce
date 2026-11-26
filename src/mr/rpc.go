package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type RequestTaskArgs struct{}

type RequestTaskReply struct {
	TaskType TaskType
	TaskID   int
	Filename string
	NReduce  int // Each mapper needs to create nreduce intermediate files for consumption
	NMap     int // Total number of Map tasks
}

type ReportTaskReply struct{}

type ReportTaskArgs struct {
	TaskType TaskType
	TaskID   int
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

// behövs just nu för att testerna ska köra 25/11, kan nog tas bort senare
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

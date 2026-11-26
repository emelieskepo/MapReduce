package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Change this to the Coordinator's private IP address when running on AWS.
const coordinatorAddr string = "127.0.0.1:1234"

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type RequestTaskArgs struct {
	WorkerID   string
	WorkerAddr string
}

type MapLocation struct {
	Addr    string
	Attempt int
}

type RequestTaskReply struct {
	TaskType TaskType
	TaskID   int
	Attempt  int
	Filename string
	NReduce  int
	NMap     int
	MapLocs  []MapLocation
}

type ReportTaskArgs struct {
	TaskType   TaskType
	TaskID     int
	Attempt    int
	WorkerID   string
	WorkerAddr string
	Output     []byte
}

type ReportTaskReply struct{}

type RegisterWorkerArgs struct {
	WorkerID   string
	WorkerAddr string
}

type RegisterWorkerReply struct{}

type ProgressTaskArgs struct {
	TaskType TaskType
	TaskID   int
	Attempt  int
	WorkerID string
	Progress float64
}

type ProgressTaskReply struct{}

type FetchArgs struct {
	MapID    int
	ReduceID int
	Attempt  int
}

type FetchReply struct {
	Data []byte
}

type GetInputArgs struct {
	Filename string
}

type GetInputReply struct {
	Data []byte
}

type ReportMapFailureArgs struct {
	MapID    int
	Attempt  int
	WorkerID string
}

type ReportMapFailureReply struct{}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

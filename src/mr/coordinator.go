package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	ID        int
	Status    TaskStatus
	StartTime time.Time
	Filename  string
}

type Coordinator struct {
	// Your definitions here.
	mu              sync.Mutex
	MapTasks        []Task
	ReduceTasks     []Task
	NReduce         int
	allMapTasksDone bool
	allReduceTasksDone bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.checkTimeOut()

	if !c.allMapTasksDone {
		for i := range c.MapTasks {
			task := &c.MapTasks[i]

			if task.Status == Idle {
				task.Status = InProgress
				task.StartTime = time.Now()

				reply.TaskType = MapTask
				reply.TaskID = task.ID
				reply.Filename = task.Filename
				reply.NReduce = c.NReduce
				reply.NMap = len(c.MapTasks)
				
				return nil
			}
		}

		if c.allMapTasksCompleted(){
			c.allMapTasksDone=true
		}else {
			reply.TaskType = WaitTask
			return nil
		}
	}
	for i := range c.ReduceTasks {
			task := &c.ReduceTasks[i]

			if task.Status == Idle {
				task.Status = InProgress
				task.StartTime = time.Now()

				reply.TaskType = ReduceTask
				reply.TaskID = task.ID
				reply.NReduce = c.NReduce
				reply.NMap = len(c.MapTasks)
				
				return nil
			}
	}

	if c.allReduceTasksCompleted(){
		c.allReduceTasksDone=true
		reply.TaskType = ExitTask
		return nil
	}else {
		reply.TaskType = WaitTask
		return nil
	}

	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if args.TaskType == MapTask{
		for i := range c.MapTasks{
			if c.MapTasks[i].ID == args.TaskID && c.MapTasks[i].Status == InProgress{
				c.MapTasks[i].Status = Completed
				break
			}
		}
		if c.allMapTasksCompleted(){
			c.allMapTasksDone = true
		}
	} else if args.TaskType == ReduceTask {
		for i := range c.ReduceTasks{
			if c.ReduceTasks[i].ID == args.TaskID && c.ReduceTasks[i].Status == InProgress{
				c.ReduceTasks[i].Status = Completed
				break
			}
		}
		if c.allReduceTasksCompleted(){
			c.allReduceTasksDone = true
		}
	}
	return nil
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
func (c *Coordinator) Done() bool { // Är det rimligt med lås här?
	c.mu.Lock()
    defer c.mu.Unlock()

    return c.allReduceTasksDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{NReduce: nReduce, allMapTasksDone: false, allReduceTasksDone: false}

	for i, file := range files {
		task := Task{
			ID:       i,
			Status:   Idle,
			Filename: file,
		}
		c.MapTasks = append(c.MapTasks, task)
	}

	for i := 0; i < nReduce; i++ {
		task := Task{
			ID:     i,
			Status: Idle,
		}
		c.ReduceTasks = append(c.ReduceTasks, task)
	}

	c.server()
	return &c
}

func (c *Coordinator) allMapTasksCompleted() bool {
	for _, task := range c.MapTasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) allReduceTasksCompleted() bool {
	for _, task := range c.ReduceTasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) checkTimeOut() {
	if !c.allMapTasksDone{
		for i := range c.MapTasks{
			task := &c.MapTasks[i]
			if task.Status == InProgress && time.Since(task.StartTime) > 10 * time.Second {
				task.Status = Idle
			}
		}
	} else if !c.allReduceTasksDone {
		for i := range c.ReduceTasks{
			task := &c.ReduceTasks[i]
			if task.Status == InProgress && time.Since(task.StartTime) > 10 * time.Second {
				task.Status = Idle
			}
		}
	}
	
}
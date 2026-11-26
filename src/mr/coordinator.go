package mr

import (
	"encoding/json"
	"fmt"
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

const (
	taskTimeout      = 10 * time.Second
	workerTimeout    = 12 * time.Second
	speculationAfter = 30 * time.Second
	stateFile        = "mr-coordinator-state.json"
)

type Task struct {
	ID           int
	Status       TaskStatus
	StartTime    time.Time
	LastProgress time.Time
	Filename     string
	OutputAddr   string
	WorkerID     string
	Attempt      int
	Progress     float64
}

type WorkerInfo struct {
	Addr     string
	LastSeen time.Time
}

type coordinatorState struct {
	MapTasks           []Task
	ReduceTasks        []Task
	NReduce            int
	AllMapTasksDone    bool
	AllReduceTasksDone bool
	InputFiles         []string
}

type Coordinator struct {
	// Your definitions here.
	mu                 sync.Mutex
	MapTasks           []Task
	ReduceTasks        []Task
	NReduce            int
	allMapTasksDone    bool
	allReduceTasksDone bool
	Workers            map[string]WorkerInfo
	InputFiles         []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Register a worker with the coordinator.
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Workers[args.WorkerID] = WorkerInfo{Addr: args.WorkerAddr, LastSeen: time.Now()}
	c.saveStateLocked()
	log.Printf("Registered worker %s at %s", args.WorkerID, args.WorkerAddr)
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Workers[args.WorkerID] = WorkerInfo{Addr: args.WorkerAddr, LastSeen: time.Now()}
	c.recoverWorkersLocked()
	c.checkTimeOutLocked()

	if !c.allMapTasksDone {
		if task := c.findMapTaskLocked(args.WorkerID); task != nil {
			c.assignTaskLocked(task, args.WorkerID)
			reply.TaskType = MapTask
			reply.TaskID = task.ID
			reply.Attempt = task.Attempt
			reply.Filename = task.Filename
			reply.NReduce = c.NReduce
			reply.NMap = len(c.MapTasks)
			c.saveStateLocked()
			return nil
		}

		if c.allMapTasksCompleted() {
			c.allMapTasksDone = true
			c.saveStateLocked()
		} else {
			reply.TaskType = WaitTask
			return nil
		}
	}

	for i := range c.ReduceTasks {
		task := &c.ReduceTasks[i]
		if task.Status == Idle {
			c.assignTaskLocked(task, args.WorkerID)
			reply.TaskType = ReduceTask
			reply.TaskID = task.ID
			reply.Attempt = task.Attempt
			reply.NReduce = c.NReduce
			reply.NMap = len(c.MapTasks)
			reply.MapLocs = c.mapLocationsLocked()
			c.saveStateLocked()
			return nil
		}
	}

	// If no idle Reduce task exists, speculate on a task that has made
	// unusually little progress. Attempt numbers prevent stale results winning.
	if task := c.findStragglingTaskLocked(c.ReduceTasks); task != nil {
		c.assignTaskLocked(task, args.WorkerID)
		reply.TaskType = ReduceTask
		reply.TaskID = task.ID
		reply.Attempt = task.Attempt
		reply.NReduce = c.NReduce
		reply.NMap = len(c.MapTasks)
		reply.MapLocs = c.mapLocationsLocked()
		c.saveStateLocked()
		return nil
	}

	if c.allReduceTasksCompleted() {
		c.allReduceTasksDone = true
		reply.TaskType = ExitTask
		c.saveStateLocked()
		return nil
	}

	reply.TaskType = WaitTask
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var tasks []Task
	if args.TaskType == MapTask {
		tasks = c.MapTasks
	} else if args.TaskType == ReduceTask {
		tasks = c.ReduceTasks
	} else {
		return fmt.Errorf("unknown task type")
	}

	if args.TaskID < 0 || args.TaskID >= len(tasks) {
		return fmt.Errorf("invalid task ID %d", args.TaskID)
	}
	task := &tasks[args.TaskID]
	if task.Status != InProgress || task.Attempt != args.Attempt || task.WorkerID != args.WorkerID {
		// A late result from an older attempt must not overwrite the current one.
		return nil
	}

	if args.TaskType == MapTask {
		task.Status = Completed
		task.OutputAddr = args.WorkerAddr
		task.Progress = 1
		log.Printf("Map task %d completed by worker %s (attempt %d)", task.ID, args.WorkerID, args.Attempt)
		if c.allMapTasksCompleted() {
			c.allMapTasksDone = true
		}
	} else {
		// Write the reduce output on the coordinator only after validating the attempt.
		outputFile := fmt.Sprintf("mr-out-%d", args.TaskID)
		tempFile, err := os.CreateTemp(".", "mr-out-tmp-*")
		if err != nil {
			return err
		}
		_, err = tempFile.Write(args.Output)
		if err != nil {
			tempFile.Close()
			os.Remove(tempFile.Name())
			return err
		}
		err = tempFile.Close()
		if err != nil {
			os.Remove(tempFile.Name())
			return err
		}
		err = os.Rename(tempFile.Name(), outputFile)
		if err != nil {
			os.Remove(tempFile.Name())
			return err
		}
		task.Status = Completed
		task.Progress = 1
		log.Printf("Reduce task %d completed", task.ID)
		if c.allReduceTasksCompleted() {
			c.allReduceTasksDone = true
		}
	}

	c.saveStateLocked()
	return nil
}

func (c *Coordinator) ProgressTask(args *ProgressTaskArgs, reply *ProgressTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var tasks []Task
	if args.TaskType == MapTask {
		tasks = c.MapTasks
	} else if args.TaskType == ReduceTask {
		tasks = c.ReduceTasks
	} else {
		return nil
	}
	if args.TaskID < 0 || args.TaskID >= len(tasks) {
		return nil
	}
	task := &tasks[args.TaskID]
	if task.Status == InProgress && task.Attempt == args.Attempt && task.WorkerID == args.WorkerID {
		now := time.Now()
		task.LastProgress = now
		if args.Progress > task.Progress && args.Progress < 1 {
			task.Progress = args.Progress
		}
		if worker, ok := c.Workers[args.WorkerID]; ok {
			worker.LastSeen = now
			c.Workers[args.WorkerID] = worker
		}
		c.saveStateLocked()
	}
	return nil
}

// A Reduce worker calls this when an intermediate file cannot be fetched.
func (c *Coordinator) ReportMapFailure(args *ReportMapFailureArgs, reply *ReportMapFailureReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.MapID < 0 || args.MapID >= len(c.MapTasks) {
		return nil
	}
	task := &c.MapTasks[args.MapID]
	if task.Status == Completed && task.Attempt == args.Attempt {
		log.Printf("Map result %d from worker %s is unavailable; scheduling it again", args.MapID, args.WorkerID)
		c.invalidateMapTaskLocked(task)
		c.saveStateLocked()
	}
	return nil
}

// Send input files to workers through RPC.
func (c *Coordinator) GetInput(args *GetInputArgs, reply *GetInputReply) error {
	c.mu.Lock()
	found := false
	for _, task := range c.MapTasks {
		if task.Filename == args.Filename {
			found = true
			break
		}
	}
	c.mu.Unlock()

	if !found {
		return fmt.Errorf("unknown input file: %s", args.Filename)
	}
	data, err := os.ReadFile(args.Filename)
	if err != nil {
		return err
	}
	reply.Data = data
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	if err := rpc.Register(c); err != nil {
		log.Fatal("RPC registration error:", err)
	}
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.allReduceTasksDone {
		// A completed job no longer needs a recovery snapshot.
		os.Remove(stateFile)
		return true
	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	if nReduce <= 0 {
		log.Fatal("nReduce must be greater than 0")
	}

	c := Coordinator{
		NReduce:    nReduce,
		Workers:    make(map[string]WorkerInfo),
		InputFiles: append([]string(nil), files...),
	}

	loaded := c.loadState(files, nReduce)
	if !loaded {
		for i, file := range files {
			c.MapTasks = append(c.MapTasks, Task{ID: i, Status: Idle, Filename: file})
		}
		for i := 0; i < nReduce; i++ {
			c.ReduceTasks = append(c.ReduceTasks, Task{ID: i, Status: Idle})
		}
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

func (c *Coordinator) assignTaskLocked(task *Task, workerID string) {
	task.Status = InProgress
	task.Attempt++
	task.WorkerID = workerID
	task.StartTime = time.Now()
	task.LastProgress = task.StartTime
	task.Progress = 0
}

func (c *Coordinator) findMapTaskLocked(workerID string) *Task {
	for i := range c.MapTasks {
		if c.MapTasks[i].Status == Idle {
			return &c.MapTasks[i]
		}
	}
	return c.findStragglingTaskLocked(c.MapTasks)
}

func (c *Coordinator) findStragglingTaskLocked(tasks []Task) *Task {
	now := time.Now()
	for i := range tasks {
		task := &tasks[i]
		if task.Status == InProgress && now.Sub(task.StartTime) > speculationAfter && task.Progress < 0.8 {
			return task
		}
	}
	return nil
}

func (c *Coordinator) mapLocationsLocked() []MapLocation {
	locations := make([]MapLocation, len(c.MapTasks))
	for i, task := range c.MapTasks {
		locations[i] = MapLocation{Addr: task.OutputAddr, Attempt: task.Attempt}
	}
	return locations
}

func (c *Coordinator) checkTimeOutLocked() {
	now := time.Now()
	for i := range c.MapTasks {
		task := &c.MapTasks[i]
		if task.Status == InProgress && now.Sub(task.LastProgress) > taskTimeout {
			task.Status = Idle
			task.WorkerID = ""
		}
	}
	for i := range c.ReduceTasks {
		task := &c.ReduceTasks[i]
		if task.Status == InProgress && now.Sub(task.LastProgress) > taskTimeout {
			task.Status = Idle
			task.WorkerID = ""
		}
	}
	if !c.allMapTasksCompleted() {
		c.allMapTasksDone = false
	}
	if !c.allReduceTasksCompleted() {
		c.allReduceTasksDone = false
	}
}

func (c *Coordinator) recoverWorkersLocked() {
	now := time.Now()
	for workerID, worker := range c.Workers {
		if now.Sub(worker.LastSeen) <= workerTimeout {
			continue
		}
		log.Printf("Worker %s appears to have failed", workerID)
		for i := range c.MapTasks {
			task := &c.MapTasks[i]
			if task.WorkerID == workerID && task.Status != Idle {
				if task.Status == Completed {
					c.invalidateMapTaskLocked(task)
				} else {
					task.Status = Idle
					task.WorkerID = ""
				}
			}
		}
		for i := range c.ReduceTasks {
			task := &c.ReduceTasks[i]
			if task.WorkerID == workerID && task.Status == InProgress {
				task.Status = Idle
				task.WorkerID = ""
			}
		}
		delete(c.Workers, workerID)
	}
	if !c.allMapTasksCompleted() {
		c.allMapTasksDone = false
	}
	if !c.allReduceTasksCompleted() {
		c.allReduceTasksDone = false
	}
}

func (c *Coordinator) invalidateMapTaskLocked(task *Task) {
	task.Status = Idle
	task.OutputAddr = ""
	task.WorkerID = ""
	task.Progress = 0
	c.allMapTasksDone = false
	c.allReduceTasksDone = false
	for i := range c.ReduceTasks {
		c.ReduceTasks[i].Status = Idle
		c.ReduceTasks[i].WorkerID = ""
		c.ReduceTasks[i].Progress = 0
	}
}

func (c *Coordinator) saveStateLocked() {
	state := coordinatorState{
		MapTasks:           c.MapTasks,
		ReduceTasks:        c.ReduceTasks,
		NReduce:            c.NReduce,
		AllMapTasksDone:    c.allMapTasksDone,
		AllReduceTasksDone: c.allReduceTasksDone,
		InputFiles:         c.InputFiles,
	}
	data, err := json.Marshal(state)
	if err != nil {
		log.Println("Could not encode coordinator state:", err)
		return
	}
	temp, err := os.CreateTemp(".", "mr-state-tmp-*")
	if err != nil {
		log.Println("Could not create coordinator state file:", err)
		return
	}
	_, err = temp.Write(data)
	if err == nil {
		err = temp.Sync()
	}
	closeErr := temp.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(temp.Name())
		log.Println("Could not write coordinator state:", err)
		return
	}
	if err = os.Rename(temp.Name(), stateFile); err != nil {
		os.Remove(temp.Name())
		log.Println("Could not replace coordinator state:", err)
	}
}

func (c *Coordinator) loadState(files []string, nReduce int) bool {
	data, err := os.ReadFile(stateFile)
	if err != nil {
		return false
	}
	var state coordinatorState
	if err = json.Unmarshal(data, &state); err != nil {
		log.Println("Ignoring invalid coordinator state:", err)
		return false
	}
	if state.NReduce != nReduce || !sameStrings(state.InputFiles, files) {
		return false
	}
	c.MapTasks = state.MapTasks
	c.ReduceTasks = state.ReduceTasks
	c.allMapTasksDone = state.AllMapTasksDone
	c.allReduceTasksDone = state.AllReduceTasksDone
	// Workers reconnect after restart. In-progress tasks are reassigned safely.
	for i := range c.MapTasks {
		if c.MapTasks[i].Status == InProgress {
			c.MapTasks[i].Status = Idle
			c.MapTasks[i].WorkerID = ""
		}
	}
	for i := range c.ReduceTasks {
		if c.ReduceTasks[i].Status == InProgress {
			c.ReduceTasks[i].Status = Idle
			c.ReduceTasks[i].WorkerID = ""
		}
	}
	c.Workers = make(map[string]WorkerInfo)
	c.saveStateLocked()
	log.Println("Restored coordinator state from disk")
	return true
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

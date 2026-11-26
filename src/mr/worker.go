package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	listener, workerAddr, err := startWorkerServer()
	if err != nil {
		log.Println("Could not start worker server:", err)
		return
	}
	defer listener.Close()

	hostname, err := os.Hostname()
	if err != nil {
		log.Println("Could not get hostname:", err)
		return
	}
	workerID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	registerArgs := RegisterWorkerArgs{WorkerID: workerID, WorkerAddr: workerAddr}
	registerReply := RegisterWorkerReply{}
	for {
		ok := call("Coordinator.RegisterWorker", &registerArgs, &registerReply)
		if ok {
			break
		}
		log.Println("Could not register worker. Retrying...")
		time.Sleep(time.Second)
	}

	log.Printf("Worker %s listening at: %s", workerID, workerAddr)

	for {
		args := RequestTaskArgs{WorkerID: workerID, WorkerAddr: workerAddr}
		reply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			time.Sleep(time.Second)
			// Re-register after a Coordinator restart.
			call("Coordinator.RegisterWorker", &registerArgs, &registerReply)
			continue
		}

		switch reply.TaskType {
		case MapTask:
			err := executeMapTask(reply.TaskID, reply.Attempt, reply.Filename, mapf,
				reply.NReduce, workerID, workerAddr)
			if err != nil {
				log.Println("Map task failed:", err)
			}
		case ReduceTask:
			err := executeReduceTask(reply.TaskID, reply.Attempt, reply.NMap,
				reply.MapLocs, reducef, workerID)
			if err != nil {
				log.Println("Reduce task failed:", err)
			}
		case WaitTask:
			time.Sleep(time.Second)
		case ExitTask:
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func startProgressReporter(taskType TaskType, taskID int, attempt int, workerID string) func() {
	stop := make(chan struct{})
	var stopped int32
	start := time.Now()
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				// Report a monotonically increasing estimate while the task runs.
				// Map/reduce functions are user callbacks and cannot expose internal progress.
				progress := float64(time.Since(start)) / float64(60*time.Second)
				if progress > 0.9 {
					progress = 0.9
				}
				args := ProgressTaskArgs{TaskType: taskType, TaskID: taskID,
					Attempt: attempt, WorkerID: workerID, Progress: progress}
				reply := ProgressTaskReply{}
				call("Coordinator.ProgressTask", &args, &reply)
			}
			if atomic.LoadInt32(&stopped) == 1 {
				return
			}
		}
	}()
	return func() {
		if atomic.CompareAndSwapInt32(&stopped, 0, 1) {
			close(stop)
		}
	}
}

func executeMapTask(
	taskID int,
	attempt int,
	filename string,
	mapf func(string, string) []KeyValue,
	nReduce int,
	workerID string,
	workerAddr string,
) error {
	stopProgress := startProgressReporter(MapTask, taskID, attempt, workerID)
	defer stopProgress()

	content, err := readInputFile(filename)
	if err != nil {
		return err
	}
	kva := mapf(filename, content)
	partitions := partitionKV(kva, nReduce)
	for i, partition := range partitions {
		err := writeIntermediateFile(taskID, i, attempt, partition)
		if err != nil {
			return err
		}
	}
	args := ReportTaskArgs{TaskType: MapTask, TaskID: taskID, Attempt: attempt,
		WorkerID: workerID, WorkerAddr: workerAddr}
	reply := ReportTaskReply{}
	if ok := call("Coordinator.ReportTask", &args, &reply); !ok {
		return fmt.Errorf("failed to report Map task %d", taskID)
	}
	return nil
}

func executeReduceTask(
	taskID int,
	attempt int,
	nMap int,
	mapLocs []MapLocation,
	reducef func(string, []string) string,
	workerID string,
) error {
	stopProgress := startProgressReporter(ReduceTask, taskID, attempt, workerID)
	defer stopProgress()

	kva, err := readIntermediateFiles(taskID, nMap, mapLocs)
	if err != nil {
		return err
	}
	output, err := writeReduceOutput(kva, reducef)
	if err != nil {
		return err
	}
	args := ReportTaskArgs{TaskType: ReduceTask, TaskID: taskID, Attempt: attempt,
		WorkerID: workerID, Output: output}
	reply := ReportTaskReply{}
	if ok := call("Coordinator.ReportTask", &args, &reply); !ok {
		return fmt.Errorf("failed to report Reduce task %d", taskID)
	}
	return nil
}

func readInputFile(filename string) (string, error) {
	args := GetInputArgs{Filename: filename}
	reply := GetInputReply{}
	if ok := call("Coordinator.GetInput", &args, &reply); !ok {
		return "", fmt.Errorf("could not fetch input file %s", filename)
	}
	return string(reply.Data), nil
}

func partitionKV(kva []KeyValue, nReduce int) [][]KeyValue {
	partitions := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		partition := ihash(kv.Key) % nReduce
		partitions[partition] = append(partitions[partition], kv)
	}
	return partitions
}

func writeIntermediateFile(taskID int, reduceID int, attempt int, partition []KeyValue) error {
	intermediateFile := fmt.Sprintf("mr-%v-%v-%v", taskID, reduceID, attempt)
	file, err := ioutil.TempFile(".", "mr-tmp-*")
	if err != nil {
		return err
	}
	tempName := file.Name()
	encoder := json.NewEncoder(file)
	for _, kv := range partition {
		if err := encoder.Encode(&kv); err != nil {
			file.Close()
			os.Remove(tempName)
			return err
		}
	}
	if err := file.Close(); err != nil {
		os.Remove(tempName)
		return err
	}
	if err := os.Rename(tempName, intermediateFile); err != nil {
		os.Remove(tempName)
		return err
	}
	return nil
}

func readIntermediateFiles(reduceID int, nMap int, mapLocs []MapLocation) ([]KeyValue, error) {
	var kva []KeyValue
	if len(mapLocs) != nMap {
		return nil, fmt.Errorf("expected %d Map locations, got %d", nMap, len(mapLocs))
	}
	for mapID := 0; mapID < nMap; mapID++ {
		location := mapLocs[mapID]
		if location.Addr == "" {
			return nil, fmt.Errorf("Map task %d has no worker address", mapID)
		}
		args := FetchArgs{MapID: mapID, ReduceID: reduceID, Attempt: location.Attempt}
		reply := FetchReply{}
		client, err := rpc.DialHTTP("tcp", location.Addr)
		if err != nil {
			reportMapFailure(mapID, location.Attempt)
			return nil, fmt.Errorf("could not connect to Map worker %d: %w", mapID, err)
		}
		err = client.Call("WorkerServer.Fetch", &args, &reply)
		client.Close()
		if err != nil {
			reportMapFailure(mapID, location.Attempt)
			return nil, fmt.Errorf("could not fetch Map %d: %w", mapID, err)
		}
		decoder := json.NewDecoder(bytes.NewReader(reply.Data))
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			kva = append(kva, kv)
		}
	}
	return kva, nil
}

func reportMapFailure(mapID int, attempt int) {
	args := ReportMapFailureArgs{MapID: mapID, Attempt: attempt}
	reply := ReportMapFailureReply{}
	call("Coordinator.ReportMapFailure", &args, &reply)
}

func writeReduceOutput(kva []KeyValue, reducef func(string, []string) string) ([]byte, error) {
	var output bytes.Buffer
	sort.Slice(kva, func(i, j int) bool { return kva[i].Key < kva[j].Key })
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		result := reducef(kva[i].Key, values)
		if _, err := fmt.Fprintf(&output, "%v %v\n", kva[i].Key, result); err != nil {
			return nil, err
		}
		i = j
	}
	return output.Bytes(), nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the rpc arguments and reply types are defined in rpc.go.
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}
	// fill in the argument(s).
	args.X = 99
	// send the RPC request, wait for the response.
	reply := ExampleReply{}
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", coordinatorAddr)
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("Dialing:", err)
		return false
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	log.Println("RPC call failed:", err)
	return false
}

// WorkerServer handles requests from other workers.
type WorkerServer struct {
	dir string
}

// Fetch sends an intermediate file to another worker.
func (w *WorkerServer) Fetch(args *FetchArgs, reply *FetchReply) error {
	filename := fmt.Sprintf("%s/mr-%d-%d-%d", w.dir, args.MapID, args.ReduceID, args.Attempt)
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	reply.Data = data
	return nil
}

// Starts the worker's RPC server.
func startWorkerServer() (net.Listener, string, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, "", err
	}
	// Create a separate RPC server for this worker.
	server := rpc.NewServer()
	if err = server.Register(&WorkerServer{dir: "."}); err != nil {
		listener.Close()
		return nil, "", err
	}
	// Connect the RPC server to HTTP.
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, server)
	mux.Handle(rpc.DefaultDebugPath, server)
	// Start serving RPC requests.
	go func() {
		if err := http.Serve(listener, mux); err != nil {
			log.Println("Worker server stopped:", err)
		}
	}()
	// Find the worker's IP address.
	host, err := getWorkerHost()
	if err != nil {
		listener.Close()
		return nil, "", err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	address := net.JoinHostPort(host, strconv.Itoa(port))
	return listener, address, nil
}

func getWorkerHost() (string, error) {
	conn, err := net.Dial("udp", coordinatorAddr)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	localAddr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return "", fmt.Errorf("could not determine worker IP address")
	}
	return localAddr.IP.String(), nil
}

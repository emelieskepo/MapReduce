package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// HÄR SKA VI IMPLEMENTERA KOD!

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

	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}

		ok := call("Coordinator.RequestTask", &args, &reply)

		if !ok {
			continue
		}

		switch reply.TaskType {
		case MapTask:
			err := executeMapTask(reply.TaskID, reply.Filename, mapf, reply.NReduce)
			if err != nil {
				log.Println("Map task failed:", err)
			}
		case ReduceTask:
			err := executeReduceTask(reply.TaskID, reply.NMap, reducef)
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

func executeMapTask(taskID int, filename string, mapf func(string, string) []KeyValue, nReduce int) error {
	content, err := readInputFile(filename)
	if err != nil {
		return err
	}
	kva := mapf(filename, content)

	partitions := partitionKV(kva, nReduce)

	for i, partition := range partitions {
		err := writeIntermediateFile(taskID, i, partition)
		if err != nil {
			return err
		}
	}

	args := ReportTaskArgs{
		TaskType: MapTask,
		TaskID:   taskID,
	}

	reply := ReportTaskReply{}

	ok := call("Coordinator.ReportTask", &args, &reply)
	if !ok {
		return fmt.Errorf("Failed to report Map task %d", taskID)
	}

	return nil
}

func executeReduceTask(taskID int, nMap int, reducef func(string, []string) string) error {
	kva, err := readIntermediateFiles(taskID, nMap)
	if err != nil {
		return err
	}

	err = writeReduceOutput(taskID, kva, reducef)
	if err != nil {
		return err
	}

	args := ReportTaskArgs{
		TaskType: ReduceTask,
		TaskID:   taskID,
	}

	reply := ReportTaskReply{}

	ok := call("Coordinator.ReportTask", &args, &reply)
	if !ok {
		return fmt.Errorf("Failed to report Reduce task %d", taskID)
	}

	return nil
}

func readInputFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}

	return string(content), nil
}

func partitionKV(kva []KeyValue, nReduce int) [][]KeyValue {
	partitions := make([][]KeyValue, nReduce)

	for _, kv := range kva {
		partition := ihash(kv.Key) % nReduce
		partitions[partition] = append(partitions[partition], kv)
	}
	return partitions
}

func writeIntermediateFile(taskID int, reduceID int, partition []KeyValue) error {
	intermediateFile := fmt.Sprintf("mr-%v-%v", taskID, reduceID)

	file, err := ioutil.TempFile(".", "mr-tmp-*")
	if err != nil {
		return err
	}

	tempName := file.Name()
	encoder := json.NewEncoder(file)

	for _, kv := range partition {
		err := encoder.Encode(&kv)
		if err != nil {
			file.Close()
			os.Remove(tempName)
			return err
		}
	}

	err = file.Close()
	if err != nil {
		os.Remove(tempName)
		return err
	}

	err = os.Rename(tempName, intermediateFile)
	if err != nil {
		os.Remove(tempName)
		return err
	}
	return nil
}

func readIntermediateFiles(reduceID int, nMap int) ([]KeyValue, error) {
	var kva []KeyValue

	for mapID := 0; mapID < nMap; mapID++ {
		filename := fmt.Sprintf("mr-%v-%v", mapID, reduceID)

		file, err := os.Open(filename)
		if err != nil {
			return nil, err
		}

		decoder := json.NewDecoder(file)

		for {
			var kv KeyValue

			err := decoder.Decode(&kv)

			if err == io.EOF {
				break
			}

			if err != nil {
				file.Close()
				return nil, err
			}

			kva = append(kva, kv)
		}

		err = file.Close()
		if err != nil {
			return nil, err
		}
	}

	return kva, nil
}

func writeReduceOutput(taskID int, kva []KeyValue, reducef func(string, []string) string) error {
	file, err := ioutil.TempFile(".", "mr-out-tmp-*")
	if err != nil {
		return err
	}

	tempName := file.Name()

	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

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

		output := reducef(kva[i].Key, values)

		_, err := fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)
		if err != nil {
			file.Close()
			os.Remove(tempName)
			return err
		}

		i = j
	}

	err = file.Close()
	if err != nil {
		os.Remove(tempName)
		return err
	}

	outputFile := fmt.Sprintf("mr-out-%v", taskID)

	err = os.Rename(tempName, outputFile)
	if err != nil {
		os.Remove(tempName)
		return err
	}

	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
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

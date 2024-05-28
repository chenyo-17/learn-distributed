package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
// copied from `mrsequential.go`
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// The worker waits this time before retrying to fetch a task
var workerWaitTime = 2 * time.Second

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// generate a unique log file for each worker
	logFile, err := os.Create(fmt.Sprintf("worker-%d.log", os.Getpid()))
	if err != nil {
		log.Fatal("Cannot create the log file")
	}
	log.SetOutput(logFile)

	// Your worker implementation here.
	for {
		taskType, taskLocations, taskNum := fetchTask()

		if taskType == "done" {
			// the worker exits
			log.Printf("A worker exits\n")
			break

		} else if taskType == "wait" {
			time.Sleep(workerWaitTime)

		} else if taskType == "map" {
			log.Printf("A worker starts a map task at %v\n", taskLocations[0])
			taskInput := taskLocations[0]
			outputFolder := doMapTask(mapf, taskInput, taskNum)
			submitMapTask(taskInput, outputFolder)

		} else if taskType == "reduce" {
			log.Printf("A worker starts a reduce task for %d\n", taskNum)
			outputFile := doReduceTask(reducef, taskLocations, taskNum)
			submitReduceTask(outputFile, taskNum)

		} else {
			log.Fatalf("Invalid task type")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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

// A worker fetches a task from the coordinator.
// The function returns the task type, the task input location(s)
// and the reduce task number if applicable.
func fetchTask() (string, []string, int) {
	args := FetchArgs{}
	reply := FetchReply{}

	// prepare the return
	taskType := ""
	taskLocations := []string{}
	taskNum := 0

	ok := call("Coordinator.Fetch", &args, &reply)

	if ok {
		taskType = reply.TaskType
		taskLocations = reply.TaskLocations
		taskNum = reply.TaskNum

	} else {
		log.Fatalf("A worker failed to fetch a task\n")
	}

	return taskType, taskLocations, taskNum
}

// The worker does the map task and returns the intermediate folder name.
func doMapTask(mapf func(string, string) []KeyValue, taskInput string, nReduce int) string {
	// load and read the input file
	file, err := os.Open(taskInput)
	if err != nil {
		log.Fatalf("A worker cannot open %v", taskInput)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("A worker cannot read %v", taskInput)
	}
	file.Close()

	// the `mapf` function returns a slice of key-value pairs
	kva := mapf(taskInput, string(content))

	// hash the key of each pair to determine its reduce task number,
	reduceTasks := make(map[int][]KeyValue)
	for _, kv := range kva {
		reduceTask := ihash(kv.Key) % nReduce
		reduceTasks[reduceTask] = append(reduceTasks[reduceTask], kv)
	}

	// sort the key-value pairs by key
	for _, kvs := range reduceTasks {
		sort.Sort(ByKey(kvs))
	}

	// create the intermediate folder
	inputPrefix := strings.TrimSuffix(filepath.Base(taskInput), filepath.Ext(taskInput))
	oFolderName := "mr-int-" + inputPrefix
	// create the folder, if it exists, remove it first
	os.RemoveAll(oFolderName)
	err = os.Mkdir(oFolderName, 0755)
	if err != nil {
		log.Fatalf("A worker cannot create %v", oFolderName)
	}

	// create all reduce task files first even if some of them will be empty
	for reduceNum := 0; reduceNum < nReduce; reduceNum++ {
		oFileName := fmt.Sprintf("%v", reduceNum)
		oFilePath := filepath.Join(oFolderName, oFileName)
		oFile, err := os.Create(oFilePath)
		if err != nil {
			log.Fatalf("A worker cannot create %v", oFilePath)
		}
		oFile.Close()
	}

	// write the sorted key-value pairs to the intermediate files
	for reduceNum, kvs := range reduceTasks {
		oFileName := fmt.Sprintf("%v", reduceNum)
		oFilePath := filepath.Join(oFolderName, oFileName)
		// open the file
		oFile, err := os.OpenFile(oFilePath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("A worker cannot open %v", oFilePath)
		}

		enc := json.NewEncoder(oFile)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("A worker cannot write %v", oFilePath)
			}
		}
		oFile.Close()
	}

	return oFolderName
}

// The worker does the reduce task and returns the output file name.
func doReduceTask(reducef func(string, []string) string,
	taskLocations []string, reduceNum int) string {

	// aggregate all values for each key
	reduceInputs := map[string][]string{}
	// load and read each reduce task file in each intermediate folder
	iFileName := fmt.Sprintf("%v", reduceNum)
	for _, taskLocation := range taskLocations {
		filePath := filepath.Join(taskLocation, iFileName)
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("A worker cannot open %v", filePath)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				// reach the end of the file
				break
			} else if err != nil {
				log.Fatalf("A worker cannot decode %v", filePath)
			}

			// TODO: this does not utilize that the key-value pairs are sorted

			reduceInputs[kv.Key] = append(reduceInputs[kv.Key], kv.Value)
		}
		file.Close()
	}

	// call `reducef` on each key and its values
	oFileName := fmt.Sprintf("mr-out-%d", reduceNum)
	os.Remove(oFileName)
	oFile, err := os.Create(oFileName)
	if err != nil {
		log.Fatalf("A worker cannot create %v", oFileName)
	}

	for key, values := range reduceInputs {
		reduceOutput := reducef(key, values)
		// write the output to the output file
		fmt.Fprintf(oFile, "%v %v\n", key, reduceOutput)
	}
	oFile.Close()

	return oFileName
}

// A worker submits a map task to the coordinator.
func submitMapTask(taskInput string, outputFolder string) {
	args := SubmitMapArgs{taskInput, outputFolder}
	reply := SubmitMapReply{}

	ok := call("Coordinator.SubmitMap", &args, &reply)
	if !ok {
		log.Fatalf("A worker failed to submit a map task\n")
	}
	log.Printf("A worker submitted the map task for %s at %s\n",
		taskInput, outputFolder)
}

// A worker does the reduce task and return the output file name.
func submitReduceTask(outputFile string, reduceNum int) {
	args := SubmitReduceArgs{reduceNum, outputFile}
	reply := SubmitReduceReply{}

	ok := call("Coordinator.SubmitReduce", &args, &reply)
	if !ok {
		log.Fatalf("A worker failed to submit a reduce task\n")
	}
	log.Printf("A worker submitted the reduce task for %d at %s\n",
		reduceNum, outputFile)
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

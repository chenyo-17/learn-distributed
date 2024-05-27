package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"

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
		task, locations, reduceNum := FetchTask()
		if task == "done" {
			break
		} else if task == "wait" {
			time.Sleep(2 * time.Second)
			continue
		} else if task == "map" {
			// TODO
			inputF := locations[0]
			fmt.Printf("The worker is processing a map task at %s\n",
				inputF)

			time.Sleep(2 * time.Second)
		} else if task == "reduce" {
			// TODO
			// collect all intermediate files
			fmt.Printf("The worker is processing a reduce task %d\n",
				reduceNum)

			time.Sleep(2 * time.Second)
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

// A worker calls this function to fetch a task from the coordinator.
// The function returns the task type and the location.
func FetchTask() (string, []string, int) {
	// empty args
	args := FetchArgs{}
	reply := FetchReply{}

	// prepare the return
	task := ""
	locations := []string{}
	reduceNum := 0

	// the connection close is handled by `call`
	ok := call("Coordinator.Fetch", &args, &reply)
	if ok {
		task = reply.TaskType
		locations = reply.TaskLocations
		if task == "reduce" {
			reduceNum = reply.ReduceNum
			fmt.Printf("The worker fetched a reduce task %d\n", reduceNum)
		} else if task == "map" {
			// one map task only includes one input file
			fmt.Printf("The worker fetched a map task at %s\n", locations[0])
		} else if task == "wait" {
			fmt.Printf("The worker waited \n")
		} else if task == "done" {
			fmt.Printf("All tasks are done\n")
		} else {
			log.Fatalf("Invalid task type")
		}
	} else {
		fmt.Printf("A worker failed to fetch a task\n")
	}
	return task, locations, reduceNum
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

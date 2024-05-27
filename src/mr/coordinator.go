package mr

import "log"
import "fmt"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

// The object registered for RPCs
type Coordinator struct {
	// Your definitions here.
	// A lock to avoid races when multiple workers try to fetch tasks
	mu sync.Mutex
	// A list of input file names yet to be processed by map tasks
	// It is initialized as all input files
	openMapTasks []string
	// A list of folder names that store the intermediate files,
	// output by each map tasks.
	// Each file name is a reduce task number.
	// Each folder stores the intermediate files of one input file
	// When all map tasks are donw, `len(doneMapTasks) == nFiles`
	doneMapTasks []string
	// A list of reduce task numbers yet to be processed by reduce tasks
	openReduceTasks []int

	// TODO: consider fault tolerance: how to recognize a dead worker
	// and reassign its unfinished tasks to other workers
}

var nFiles int
var nReduce int

// Your code here -- RPC handlers for the worker to call.

// When a worker asks for a task, the coordinator either assigns a map task
// by replying one file name, or if all map tasks are done,
// assigns a reduce task by replying a reduce task number.
func (c *Coordinator) Fetch(args *FetchArgs, reply *FetchReply) error {
	c.mu.Lock()
	// unlock the mutex before returning
	defer c.mu.Unlock()

	// assign a map task if there exists any
	if len(c.openMapTasks) > 0 {
		reply.TaskType = "map"
		reply.TaskLocations = []string{c.openMapTasks[0]}
		// update the status
		c.openMapTasks = c.openMapTasks[1:]
		fmt.Printf("The coordinator assigned a map task at %s\n",
			reply.TaskLocations[0])
	} else if len(c.doneMapTasks) < nFiles {
		// no map task is available, but not all map tasks are done
		// ask the worker to wait
		reply.TaskType = "wait"
		fmt.Printf("The coordinator asked the worker to wait\n")
	} else if len(c.openReduceTasks) > 0 {
		// assign a reduce task if all map tasks are done
		// and there exists any reduce task
		reply.TaskType = "reduce"
		// copy all intermediate folder names to the reply
		// TODO: consider only replying the point, which
		// is cheaper but requires adapting the reply struct
		reply.TaskLocations = make([]string, len(c.doneMapTasks))
		copy(reply.TaskLocations, c.doneMapTasks)
		// assign a reduce task number
		reply.ReduceNum = c.openReduceTasks[0]
		c.openReduceTasks = c.openReduceTasks[1:]
		fmt.Printf("The coordinator assigned a reduce task %d\n",
			reply.ReduceNum)
	} else if len(c.openReduceTasks) == 0 {
		// all tasks are done
		reply.TaskType = "done"
		fmt.Printf("All tasks are done\n")
	} else {
		// should never reach here
		log.Fatalf("Invalid state")
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// c := Coordinator{}

	// Your code here.
	// initialize the coordinator
	c := Coordinator{openMapTasks: files}
	nFiles = len(files)
	nReduce = nReduce
	for i := 0; i < nReduce; i++ {
		c.openReduceTasks = append(c.openReduceTasks, i)
	}

	c.server()
	return &c
}

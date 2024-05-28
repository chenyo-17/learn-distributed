package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

// The object registered for RPCs
type Coordinator struct {
	// Your definitions here.
	// A lock to avoid races when multiple workers try to fetch tasks
	mu sync.Mutex
	// A list of input file names yet to be processed by map tasks
	// It is initialized as all input files
	openMapTasks []string
	// Record the lapsed time of each on going task
	// A list of folder names that store the intermediate files,
	// output by each map tasks.
	// Each file name is a reduce task number.
	// Each folder stores the intermediate files of one input file
	// When all map tasks are donw, `len(doneMapTasks) == nFiles`
	doneMapTasks []string
	// A list of reduce task numbers yet to be processed by reduce tasks
	openReduceTasks []int
	// A list of reduce task numbers that have been processed by reduce tasks,
	// used to determine if all tasks are done
	doneReduceTasks []int
}

var nFiles int
var nReduceTasks int

// Your code here -- RPC handlers for the worker to call.

// When a worker asks for a task, the coordinator either assigns a map task
// by replying one file name, or if all map tasks are done,
// assigns a reduce task by replying a reduce task number.
func (c *Coordinator) Fetch(args *FetchArgs, reply *FetchReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// assign a map task if there exists any
	if len(c.openMapTasks) > 0 {
		reply.TaskType = "map"
		reply.TaskLocations = []string{c.openMapTasks[0]}
		reply.TaskNum = nReduceTasks

		// update the status
		c.openMapTasks = c.openMapTasks[1:]
		log.Printf("The coordinator assigned a map task at %s\n",
			reply.TaskLocations[0])

		// create a goroutine to monitor the task status
		go c.monitorMapTaskStatus(reply.TaskLocations[0])

	} else if len(c.doneMapTasks) < nFiles {
		// no map task is available, but not all map tasks are done
		reply.TaskType = "wait"
		log.Printf("The coordinator asked the worker to wait\n")

	} else if len(c.openReduceTasks) > 0 {
		// assign a reduce task if all map tasks are done
		// and there exists any reduce task
		reply.TaskType = "reduce"

		// TODO: consider only replying the point, which
		// is cheaper but requires adapting the reply struct

		// copy all intermediate folder names to the reply
		reply.TaskLocations = make([]string, len(c.doneMapTasks))
		copy(reply.TaskLocations, c.doneMapTasks)

		// assign a reduce task number
		reply.TaskNum = c.openReduceTasks[0]
		c.openReduceTasks = c.openReduceTasks[1:]
		log.Printf("The coordinator assigned a reduce task %d\n", reply.TaskNum)

		// create a goroutine to monitor the task status
		go c.monitorReduceTaskStatus(reply.TaskNum)

	} else if len(c.doneReduceTasks) < nReduceTasks {
		reply.TaskType = "wait"
		// log.Printf("The coordinator asked the worker to wait\n")

	} else if len(c.doneReduceTasks) == nReduceTasks {
		reply.TaskType = "done"
		// log.Printf("The coordinator informed the worker that all tasks are done\n")

		// clear intermediate folders
		for _, folder := range c.doneMapTasks {
			os.RemoveAll(folder)
		}

	} else {
		log.Fatalf("Invalid state")
	}

	return nil
}

// When a worker submits a map task, the coordinator updates the task status,
func (c *Coordinator) SubmitMap(args *SubmitMapArgs, reply *SubmitMapReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	taskInput := args.TaskInput
	submitLocation := args.SubmitLocation

	// remove the task from the `openMapTasks`
	for i, task := range c.openMapTasks {
		if task == taskInput {
			c.openMapTasks = append(c.openMapTasks[:i],
				c.openMapTasks[i+1:]...)
			break
		}
	}

	// add the task to the `doneMapTasks`
	// the worker only informs the coordinate the folder location once it submits the job
	// so the coordinator never stores unfinished or crashed folder locations
	c.doneMapTasks = append(c.doneMapTasks, submitLocation)
	log.Printf("The coordinator received the map task for %s at %s\n",
		taskInput, submitLocation)

	return nil
}

func (c *Coordinator) SubmitReduce(args *SubmitReduceArgs, reply *SubmitReduceReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reduceNum := args.ReduceNum
	submitLocation := args.SubmitLocation

	// remove the task from the `openReduceTasks`
	for i, task := range c.openReduceTasks {
		if task == reduceNum {
			c.openReduceTasks = append(c.openReduceTasks[:i],
				c.openReduceTasks[i+1:]...)
			break
		}
	}

	// add the task to the `doneReduceTasks`
	c.doneReduceTasks = append(c.doneReduceTasks, reduceNum)
	log.Printf("The coordinator received the reduce task for %d at %s\n",
		reduceNum, submitLocation)

	return nil
}

// Maximum waiting time for a task to be done
var MaxTaskDuration = 10 * time.Second

// TODO: consider more generic functions to monior the status of any task

// Whenever the coordinator assigns a map task, it also creates a goroutine
// to monitor the task status. It checks the task status after `MaxTaskTime` second,
// if the task is not done, the coordinator will reassign the task.
// If the task has been done, the goroutine will do nothing and exit.
func (c *Coordinator) monitorMapTaskStatus(task string) {
	time.Sleep(MaxTaskDuration)
	// check if the task is done
	if !c.isMapTaskDone(task) {
		// reassign the task later
		c.mu.Lock()
		defer c.mu.Unlock()
		c.openMapTasks = append(c.openMapTasks, task)
		log.Printf("The coordinator timeout the map task for %s\n", task)

	}
}

// Monitor the status of a reduce task,
// the logic is similar to `monitorMapTaskStatus`
func (c *Coordinator) monitorReduceTaskStatus(task int) {
	time.Sleep(MaxTaskDuration)
	// check if the task is done
	if !c.isReduceTaskDone(task) {
		// only try to get the lock after `isReduceTaskDone` releases the lock
		c.mu.Lock()
		defer c.mu.Unlock()
		c.openReduceTasks = append(c.openReduceTasks, task)
		log.Printf("The coordinator timeout the reduce task for %d\n", task)

	}
}

// Check if a map task is done
func (c *Coordinator) isMapTaskDone(task string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, doneTask := range c.doneMapTasks {
		if doneTask == task {
			return true
		}
	}
	return false
}

// Check if a reduce task is done
func (c *Coordinator) isReduceTaskDone(task int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, doneTask := range c.doneReduceTasks {
		if doneTask == task {
			return true
		}
	}
	return false
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

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.doneMapTasks) == nFiles && len(c.doneReduceTasks) == nReduceTasks {
		ret = true
	}

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// c := Coordinator{}

	// remove all files ending with `.log`
	for _, file := range files {
		if file[len(file)-4:] == ".log" {
			os.Remove(file)
		}
	}

	// create a new log file
	logFile, err := os.Create("coordinator.log")
	if err != nil {
		log.Fatal("Cannot create the log file")
	}
	log.SetOutput(logFile)

	// Your code here.
	// initialize the coordinator
	c := Coordinator{openMapTasks: files}
	nFiles = len(files)
	nReduceTasks = nReduce
	for i := 0; i < nReduce; i++ {
		c.openReduceTasks = append(c.openReduceTasks, i)
	}

	c.server()
	return &c
}

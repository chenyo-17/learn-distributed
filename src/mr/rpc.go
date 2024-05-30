package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.

// No args are needed when a work feches a task
type FetchArgs struct{}

// The coordinator reply to a worker's FetchTask request
type FetchReply struct {
	// either "map" or "reduce" or "done"
	// "done" means all tasks are done
	TaskType string
	// the reply for a map task is only one input file name
	// the reply for a reduce task is a list of intermediate folder names
	TaskLocations []string
	// for a map task, this field indicates the total number of reduce tasks to split
	// for a reduce task, this field indicates the reduce task number
	TaskNum int
}

// A worker submit a map task to the coordinator
type SubmitMapArgs struct {
	// the input file name
	TaskInput string
	// the intermediate folder name
	SubmitLocation string
}

// No reply from the coordinator when a worker submits a map task
type SubmitMapReply struct{}

// A worker submit a reduce task to the coordinator
type SubmitReduceArgs struct {
	// the reduce task number
	ReduceNum int
	// the output file name
	SubmitLocation string
}

// No reply from the coordinator when a worker submits a reduce task
type SubmitReduceReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

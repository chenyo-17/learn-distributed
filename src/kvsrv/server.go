package kvsrv

import (
	"log"
	"os"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Ack struct {
	Seq   int
	Value string
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.

	// the shared map
	kvs map[string]string
	// a map from a pid to the last seq no. the server has replied
	// and the reply (i.e., `Value`) itself
	acks map[int]Ack
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	cur := args.Seq
	last := kv.acks[args.Pid].Seq

	DPrintf("%v gets the value of %v in seq no. %v", args.Pid, args.Key, cur)

	// Assume the client keeps sending the same request until it receives the reply
	// either `cur == last` or `cur == last + 1`
	if cur == last {
		reply.Value = kv.acks[args.Pid].Value
		DPrintf("Return to %v the cached value for %v: %v", args.Pid, args.Key, reply.Value)
	} else {
		// if cur != last+1 {
		// 	log.Fatalf("The server receives an out-of-order seq no.: %v, last: %v", cur, last)
		// }
		// new request
		// return "" if the key is not stored
		reply.Value = kv.kvs[args.Key]
		DPrintf("Return to %v the latest value from the kvs for %v: %v", args.Pid, args.Key, reply.Value)
		// update `kv.acks`
		// kv.acks[args.Pid] = Ack{Seq: cur, Value: reply.Value}
		// DPrintf("Update the cache for %v: seq: %v, value: %v", args.Pid, cur, reply.Value)

	}
	// DPrintf("Get(%v) = %v", args.Key, reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("%v puts the value of %v in seq no %v: %v", args.Pid, args.Key, args.Seq, args.Value)

	cur := args.Seq
	last := kv.acks[args.Pid].Seq

	if cur == last {
		reply.Value = kv.acks[args.Pid].Value
		DPrintf("Return to %v the cached value for %v: %v", args.Pid, args.Key, reply.Value)
	} else {
		// if cur != last+1 {
		// 	log.Fatalf("The server receives an out-of-order seq no.: %v, last: %v", cur, last)
		// }
		kv.kvs[args.Key] = args.Value
		DPrintf("Update the kvs for %v: %v", args.Key, args.Value)
		reply.Value = kv.kvs[args.Key]
		DPrintf("Return to %v the latest value from the kvs for %v: %v", args.Pid, args.Key, reply.Value)
		// kv.acks[args.Pid] = Ack{Seq: cur, Value: reply.Value}
		// DPrintf("Update the cache for %v: seq: %v, value: %v", args.Pid, cur, reply.Value)
	}

	// DPrintf("Put(%v) = %v", args.Key, reply.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("%v appends the value of %v in seq no. %v: %v", args.Pid, args.Key, args.Seq, args.Value)

	cur := args.Seq
	last := kv.acks[args.Pid].Seq

	if cur == last {
		reply.Value = kv.acks[args.Pid].Value
		DPrintf("Return to %v the cached value for %v: %v", args.Pid, args.Key, reply.Value)
	} else {
		// if cur != last+1 {
		// 	log.Fatalf("The server receives an out-of-order seq no.: %v, last: %v", cur, last)
		// }
		// returns old value!
		reply.Value = kv.kvs[args.Key]
		DPrintf("Return to %v the old value from the kvs for %v: %v", args.Pid, args.Key, reply.Value)
		kv.kvs[args.Key] = kv.kvs[args.Key] + args.Value
		DPrintf("Update the kvs for %v: %v", args.Key, kv.kvs[args.Key])
		// kv.acks[args.Pid] = Ack{Seq: cur, Value: reply.Value}
		// DPrintf("Update the cache for %v: seq: %v, value: %v", args.Pid, cur, reply.Value)
	}

	// DPrintf("Append(%v) = %v", args.Key, reply.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.acks = make(map[int]Ack)

	logFile, err := os.Create("server.log")
	if err != nil {
		log.Fatal("Cannot create the log file")
	}
	log.SetOutput(logFile)

	return kv
}

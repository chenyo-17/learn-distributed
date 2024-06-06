package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.

	// the shared map
	kvs map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// return "" if the key is not stored
	reply.Value = kv.kvs[args.Key]
	DPrintf("Get(%v) = %v", args.Key, reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.kvs[args.Key] = args.Value
	reply.Value = kv.kvs[args.Key]
	DPrintf("Put(%v) = %v", args.Key, reply.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// returns old value!
	reply.Value = kv.kvs[args.Key]
	kv.kvs[args.Key] = kv.kvs[args.Key] + args.Value
	DPrintf("Append(%v) = %v", args.Key, reply.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)

	return kv
}

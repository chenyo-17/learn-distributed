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

type Ack struct {
	// the last seq no. the server has replied
	Seq int64
	// the reply (i.e., `Value`) itself, only for `Append`
	Value string
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.

	// the shared map
	kvs map[string]string
	// a map from a pid to the last seq no. the server has replied
	// and the reply (i.e., `Value`) itself
	acks map[int64]Ack
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// `Get` can always return the latest value
	// this satisfies the linearizability, according to example 7 in the lec 4 notes
	reply.Value = kv.kvs[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	cur := args.Seq
	last := kv.acks[args.Cid].Seq

	// if the server has seen the seq no. before, do nothing
	// (`Put` does not need a reply)
	// if not, execute the request
	// `cur` may not be `last+1` as `Get` request is not recorded
	if cur > last {
		kv.kvs[args.Key] = args.Value
		// only record the seq. no, not value is needed
		// this is a client only sends one request a time
		kv.acks[args.Cid] = Ack{Seq: cur}
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	cur := args.Seq
	last := kv.acks[args.Cid].Seq

	if cur == last {
		reply.Value = kv.acks[args.Cid].Value
	} else {
		reply.Value = kv.kvs[args.Key]
		kv.kvs[args.Key] = kv.kvs[args.Key] + args.Value
		kv.acks[args.Cid] = Ack{Seq: cur, Value: reply.Value}
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.acks = make(map[int64]Ack)
	return kv
}

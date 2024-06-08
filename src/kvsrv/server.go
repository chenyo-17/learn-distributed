package kvsrv

import (
	"log"
	"os"
	"strings"
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
	Seq   int64
	Value string
	// "Put" or "Append"
	Op string
	// the key for the value
	Key string
	// when a client sends a new request,
	// the server should return the current `kvs` value,
	// in this case, `Latest` is set to true, and `Value` is ignored
	Latest bool
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.

	// the shared map
	kvs map[string]string
	// a map from a pid to the last seq no. the server has replied
	// and the reply (i.e., `Value`) itself
	acks map[int64]Ack
	// store the last appended value for each key
	// this is used to compute the cached value for each ack
	lastAppend map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	cur := args.Seq
	last := kv.acks[args.Cid].Seq

	DPrintf("%v gets the value of %v in seq no. %v:", args.Cid, args.Key, cur)

	// Assume the client keeps sending the same request until it receives the reply
	// either `cur == last` or `cur == last + 1`
	if cur == last {
		if kv.acks[args.Cid].Latest {
			reply.Value = kv.kvs[args.Key]
		} else {
			reply.Value = kv.acks[args.Cid].Value
		}
		DPrintf("Return to %v the cached value for %v", args.Cid, args.Key)
	} else {
		if cur != last+1 {
			log.Fatalf("The server receives an out-of-order seq no.: %v, last: %v", cur, last)
		}
		// new request
		// return "" if the key is not stored
		reply.Value = kv.kvs[args.Key]
		// DPrintf("Return to %v the latest value from the kvs for %v: %v", args.Pid, args.Key, reply.Value)
		// a new request gets the latest value
		kv.acks[args.Cid] = Ack{Seq: cur, Op: "Get", Key: args.Key, Latest: true}
		DPrintf("Update the cache for %v: seq: %v", args.Cid, cur)

	}
	// DPrintf("Get(%v) = %v", args.Key, reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("%v puts %v in seq no %v", args.Cid, args.Key, args.Seq)

	cur := args.Seq
	last := kv.acks[args.Cid].Seq

	if cur == last {
		if kv.acks[args.Cid].Latest {
			reply.Value = kv.kvs[args.Key]
		} else {
			reply.Value = kv.acks[args.Cid].Value
		}
		DPrintf("Return to %v the cached value for %v", args.Cid, args.Key)
	} else {
		if cur != last+1 {
			log.Fatalf("The server receives an out-of-order seq no.: %v, last: %v", cur, last)
		}
		// before the server updates the `kvs`, it should first
		// cache the old value in `acks`, and the ack is no more the latest
		for cid, ack := range kv.acks {
			if cid == args.Cid {
				continue
			}
			if ack.Op == "Put" && ack.Key == args.Key && ack.Latest {
				// // these acks are no more the latest
				kv.acks[cid] = Ack{Seq: kv.acks[cid].Seq, Op: "Put", Key: args.Key, Latest: false, Value: kv.kvs[args.Key]}
				// ack.Value = kv.kvs[args.Key]
				// ack.Latest = false
			} else if ack.Op == "Append" && ack.Key == args.Key && ack.Latest {
				// the old value should exclude the last appended value
				// ack.Value = strings.TrimSuffix(kv.kvs[args.Key], kv.lastAppend[args.Key])
				// ack.Latest = false
				kv.acks[cid] = Ack{Seq: kv.acks[cid].Seq, Op: "Append", Key: args.Key,
					Latest: false,
					Value:  strings.TrimSuffix(kv.kvs[args.Key], kv.lastAppend[args.Key])}
				DPrintf("Update other cache for %v: %v", cid, kv.acks[cid])
			}
		}
		kv.kvs[args.Key] = args.Value
		DPrintf("Update the kvs for %v: %v", args.Key, kv.kvs[args.Key])
		reply.Value = kv.kvs[args.Key]
		DPrintf("Return to %v the latest value from the kvs for %v", args.Cid, args.Key)
		kv.acks[args.Cid] = Ack{Seq: cur, Op: "Put", Key: args.Key, Latest: true}
		DPrintf("Update the cache for %v: seq: %v", args.Cid, cur)
	}

	// DPrintf("Put(%v) = %v", args.Key, reply.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("%v appends the value of %v in seq no. %v: %v", args.Cid, args.Key, args.Seq, args.Value)

	cur := args.Seq
	last := kv.acks[args.Cid].Seq

	if cur == last {
		if kv.acks[args.Cid].Latest {
			reply.Value = strings.TrimSuffix(kv.kvs[args.Key], kv.lastAppend[args.Key])
			DPrintf("%v gets the latest value of %v: %v", args.Cid, args.Key, reply.Value)
		} else {
			reply.Value = kv.acks[args.Cid].Value
			DPrintf("Return to %v the cached value for %v", args.Cid, args.Key)
		}
	} else {
		if cur != last+1 {
			log.Fatalf("The server receives an out-of-order seq no.: %v, last: %v", cur, last)
		}
		for cid, ack := range kv.acks {
			if cid == args.Cid {
				continue
			}
			if ack.Op == "Put" && ack.Key == args.Key && ack.Latest {
				// these acks are no more the latest
				kv.acks[cid] = Ack{Seq: kv.acks[cid].Seq, Op: "Append", Key: args.Key, Latest: false, Value: kv.kvs[args.Key]}
				// ack.Value = kv.kvs[args.Key]
				// ack.Latest = false
			} else if ack.Op == "Append" && ack.Key == args.Key && ack.Latest {
				// the old value should exclude the last appended value
				kv.acks[cid] = Ack{Seq: kv.acks[cid].Seq, Op: "Append", Key: args.Key,
					Latest: false,
					Value:  strings.TrimSuffix(kv.kvs[args.Key], kv.lastAppend[args.Key])}
				// ack.Value = strings.TrimSuffix(kv.kvs[args.Key], kv.lastAppend[args.Key])
				// ack.Latest = false
				DPrintf("Update the other cache for %v: %v", cid, kv.acks[cid])
			}
		}
		DPrintf("Receive a new request for %v from %v", args.Key, args.Cid)
		// returns old value!
		reply.Value = kv.kvs[args.Key]
		DPrintf("Return to %v the old value for %v: %v", args.Cid, args.Key, reply.Value)
		kv.kvs[args.Key] = kv.kvs[args.Key] + args.Value
		kv.lastAppend[args.Key] = args.Value
		DPrintf("Update the kvs for %v: %v", args.Key, kv.kvs[args.Key])
		DPrintf("Update the lastAppend for %v: %v", args.Key, kv.lastAppend[args.Key])
		kv.acks[args.Cid] = Ack{Seq: cur, Op: "Append", Key: args.Key, Latest: true}
		DPrintf("Update the cache for %v: seq: %v: latest", args.Cid, cur)
	}

	// DPrintf("Append(%v) = %v", args.Key, reply.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.acks = make(map[int64]Ack)
	kv.lastAppend = make(map[string]string)

	logFile, err := os.Create("server.log")
	if err != nil {
		log.Fatal("Cannot create the log file")
	}
	log.SetOutput(logFile)

	return kv
}

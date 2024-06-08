package kvsrv

import (
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
)

// cannot be put in `Clerk` struct!

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	cid int64
	seq int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.cid = nrand()
	ck.seq = 0
	return ck
}

// fetch the current value for a key. returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.seq = ck.seq + 1
	args := GetArgs{Key: key, Cid: ck.cid, Seq: ck.seq}
	reply := GetReply{}

	// keep trying until ok
	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			return reply.Value
		}
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	// increase the seq number by 1 for every new request
	ck.seq = ck.seq + 1
	args := PutAppendArgs{Key: key, Value: value, Cid: ck.cid, Seq: ck.seq}
	reply := PutAppendReply{}

	for {
		ok := false
		switch op {
		case "Put":
			ok = ck.server.Call("KVServer.Put", &args, &reply)
		case "Append":
			ok = ck.server.Call("KVServer.Append", &args, &reply)
		}
		if ok {
			return reply.Value
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

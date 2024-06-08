package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// The unique client id
	Cid int64
	// A sequence number like in tcp,
	// increase by one for each new request
	Seq int64
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	// Return the value after the operation
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cid int64
	Seq int64
}

type GetReply struct {
	Value string
}

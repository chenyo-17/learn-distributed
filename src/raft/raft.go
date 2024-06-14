package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// An example log command
type KVCommand struct {
	Term  int    // in which term the entry is appended
	Op    string // "PUT" or "GET"
	Key   string // the key to put or get
	Value string // the value to put
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int          // the latest term this peer has seen
	votedFor    int          // who this peer has voted for in the current current
	log         []*KVCommand // log entries
	commitIndex int          // the latest committed log entry the server knows
	lastApplied int          // the latest log entry the server has applied to its state machine

	// for a leader, reinitialized after each election
	nextIndex  []int // the next log entry to send to each follower
	matchIndex []int // the index of the highest log entry known to be replicated on each server

	// the following fields are not in the Raft paper Figure 2
	lastLogIndex int // the last log index
	lastLogTerm  int // the last log term

	validAppendReceived bool // whether a Valid AppendEntries is received
	voteGranted         bool // whether the server has granted a vote

	receivedVotes int // the number of votes received in one election

	isCandidate bool // whether the server is a candidate
	isLeader    bool // whether the server is a leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.isLeader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // next term
	CandidateId  int
	LastLogIndex int // candidate's latest log entry
	LastLogTerm  int // candidate's latest log term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // a server's `currentTerm`
	VoteGranted bool // whether the follower accepts the vote request
}

// example RequestVote RPC handler.
// A server handles a received vote request
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if rf.killed() {
		reply.VoteGranted = false
	} else {
		// 1. check whether `Term >= rf.currentTerm`
		// 2. check whether the server has voted for someone else
		// 3. check whether the candidate has up-to-date log
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term < rf.currentTerm {
			reply.VoteGranted = false
		} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			reply.VoteGranted = false
		} else if args.LastLogTerm > rf.commitIndex ||
			(args.LastLogTerm == rf.commitIndex && args.LastLogIndex >= rf.lastLogIndex) {
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.voteGranted = true
		} else {
			reply.VoteGranted = false
		}
		// 4. check whether the own term is outdated
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			// fall back to a follower
			rf.isLeader = false
			rf.isCandidate = false
		}
		DPrintf("Server %d receives a vote request from candidate %d, args: %v, reply: %v", rf.me, args.CandidateId, args, reply)
	}
}

// AppendEntries RPC args
type AppendEntriesArgs struct {
	Term         int // the leader's term
	LeaderId     int
	LastLogIndex int // the last log entry before the new ones
	LastLogTerm  int // the term of the last log entry
	// Entries      []*KVCommand // a list of log entries to append, empty for the heartbeat
	LeaderCommit int // the leader's `CommitIndex`
}

type AppendEntriesReply struct {
	Term    int  // a server's `currentTerm`
	Success bool // whether the server accepts
}

// A server handles an AppendEntries request
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Success = false
	} else {
		// 1. check whether `args.Term >= rf.currentTerm`
		// 2. check whether the server's log entry at `args.PrevLogIndex` is `args.PrevLogTerm`
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term < rf.currentTerm {
			reply.Success = false
		} else if rf.lastLogIndex < args.LastLogIndex || rf.lastLogTerm != args.LastLogTerm {
			reply.Success = false
		} else {
			reply.Success = true
			rf.validAppendReceived = true
			// TODO: 3. modify `rf.log` and other fields for non heartbeats
			// 4. update `rf.commitIndex`
			if args.LeaderCommit > rf.commitIndex {
				// no `min` for Go 1.20
				if args.LeaderCommit < rf.lastLogIndex {
					rf.commitIndex = rf.lastLogIndex
				} else {
					rf.commitIndex = args.LeaderCommit
				}
			}
		}
		// 5. check whether the own term is outdated
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			// fall back to a follower
			rf.isLeader = false
			rf.isCandidate = false
		}
		DPrintf("Server %d receives an AppendEntries request from leader %d, args: %v, reply: %v", rf.me, args.LeaderId, args, reply)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int) {
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}

	rf.mu.Lock()
	// 1. increment the current term: handled in `ticker`
	// 2. vote for self
	rf.votedFor = rf.me
	// 3. reset election timer: handled in `ticker`

	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.lastLogIndex
	args.LastLogTerm = rf.lastLogTerm
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if ok {
		rf.mu.Lock()
		// compare the terms
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			// fall back to a follower
			rf.isLeader = false
			rf.isCandidate = false
			DPrintf("Candidate %d falls back to a follower due to the outdated term", rf.me)
		} else if reply.VoteGranted {
			rf.receivedVotes += 1
			DPrintf("Candidate %d receives a vote from server %d", rf.me, server)
		}
		rf.mu.Unlock()
	}
}

// A server sends an sendAppendEntries RPC to another server
func (rf *Raft) sendAppendEntries(server int) {
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	rf.mu.Lock()

	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastLogIndex = rf.lastLogIndex
	args.LastLogTerm = rf.lastLogTerm
	args.LeaderCommit = rf.commitIndex

	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if ok {
		rf.mu.Lock()
		// compare the terms
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			// fall back to a follower
			rf.isLeader = false
			rf.isCandidate = false
			DPrintf("Leader %d falls back to a follower due to the outdated term", rf.me)
		}
		rf.mu.Unlock()
	}
}

// Update the server to be a leader
func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.isLeader = true

	// initialize the leader's state
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastLogIndex + 1
	}

	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	heartbeatPeriod := time.Duration(100) * time.Millisecond
	for rf.killed() == false {

		// Your code here (3A)

		// if the server is a leader, send the heartbeats periodically
		if rf.isLeader {
			// a leader is never replaced unless it is killed
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				// send the heartbeat
				go rf.sendAppendEntries(i)
			}
			time.Sleep(heartbeatPeriod)

		} else if rf.isCandidate {
			// wait to gather enough votes
			electionTimeout := 500 + (rand.Int63() % 800)
			time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

			// get the current states
			rf.mu.Lock()
			validAppendReceived := rf.validAppendReceived
			receivedVotes := rf.receivedVotes
			// reset
			rf.validAppendReceived = false
			rf.receivedVotes = 0
			rf.mu.Unlock()

			// first check whether a new leader exists
			if validAppendReceived {
				// fall back to a follower
				rf.isCandidate = false
				DPrintf("Candidate %d falls back to a follower", rf.me)
			} else {
				// check the votes
				if receivedVotes >= (len(rf.peers)-1)/2 {
					// becomes a leader
					rf.becomeLeader()
					DPrintf("Candidate %d becomes a leader with %d votes", rf.me, receivedVotes)
				} else {
					// remain as a candidate
					DPrintf("Candidate %d remains a candidate", rf.me)
					// resends the vote requests
					rf.mu.Lock()
					rf.currentTerm += 1
					rf.mu.Unlock()
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						go rf.sendRequestVote(i)
					}
				}
			}
		} else { // the server is a follower
			// wait for the election timeout
			electionTimeout := 500 + (rand.Int63() % 800)
			time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

			// get the current states
			rf.mu.Lock()
			validAppendReceived := rf.validAppendReceived
			voteGranted := rf.voteGranted
			// reset
			rf.validAppendReceived = false
			rf.voteGranted = false
			rf.mu.Unlock()

			if validAppendReceived || voteGranted {
				// remain as a follower
				DPrintf("Follower %d remains a follower", rf.me)
				continue
			} else {
				// become a candidate start an election
				rf.isCandidate = true
				DPrintf("Follower %d becomes a candidate", rf.me)
				// incremnt the term before sending the vote requests
				rf.mu.Lock()
				rf.currentTerm += 1
				rf.mu.Unlock()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go rf.sendRequestVote(i)
				}
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.dead = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = nil
	// first log index is 1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastLogIndex = 0
	rf.nextIndex = nil
	rf.matchIndex = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

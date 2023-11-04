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
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/MarcValentino/trabalho2_SD/src/labrpc"
)

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *Persister          // Object to hold this peer's persisted state
	me             int                 // this peer's index into peers[]
	ElectionTimer  *time.Timer
	CurrentTerm    int // current term held as an int
	VotedFor       int // id of the candidate this node voted for
	IsLeader       bool
	HeartbeatTimer *time.Timer
	// o atributo log[] e os outros que representam estado não serão implementados
	// porque não são necessários para a eleição ocorrer.
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
// pergunta: o que isso quer dizer?
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	term = rf.CurrentTerm
	isLeader = rf.IsLeader
	// Your code here (2A).
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

type RequestVoteArgs struct {
	Term        int
	CandidateId int

	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool

	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//

// Implement the RequestVote() RPC handler so that servers will vote for
// one another.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.ElectionTimer.Reset(time.Duration(2+int(4*rand.Float32())) * time.Second)
	if rf.CurrentTerm < args.Term {
		if rf.IsLeader {
			rf.IsLeader = false
		}
		rf.VotedFor = args.CandidateId
		rf.CurrentTerm = args.Term
		reply.Term = args.Term
		reply.VoteGranted = true

	} else {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	}

	// Your code here (2A, 2B).
}

//
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
//

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Ok bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.ElectionTimer.Reset(time.Duration(2+int(4*rand.Float32())) * time.Second)
	reply.Ok = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) sendAllVotes() {
	rf.VotedFor = rf.me
	print(fmt.Sprintf("node %d is candidate\n", (*rf).me))
	voteArgs := RequestVoteArgs{}
	rf.CurrentTerm += 1
	voteArgs.CandidateId = rf.me
	voteArgs.Term = rf.CurrentTerm
	fmt.Printf("voteArgs: %+v\n", voteArgs)
	voteReply := RequestVoteReply{}
	voteCount := 1
	for i := 0; i < len(rf.peers); i++ {
		rf.sendRequestVote(i, &voteArgs, &voteReply)
		fmt.Printf("voteReply: %+v\n", voteReply)
		if voteReply.VoteGranted {
			voteCount += 1
			if voteCount > int(len(rf.peers)/2) {
				print("leader elected")
				rf.IsLeader = true
				break
			}
		} else if voteReply.Term > rf.CurrentTerm {
			rf.CurrentTerm = voteReply.Term
			break

		}
	}
	rf.ElectionTimer.Reset(time.Duration(2+int(4*rand.Float32())) * time.Second)

}

func (rf *Raft) sendAllAppendEntries() {
	if rf.IsLeader {
		appendEntriesArgs := AppendEntriesArgs{}
		appendEntriesArgs.LeaderId = rf.me
		appendEntriesArgs.Term = rf.CurrentTerm
		appendEntriesReply := AppendEntriesReply{}
		for i := 0; i < len(rf.peers); i++ {
			rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply)
			fmt.Printf("appendEntriesReply: %+v\n", appendEntriesReply)
		}
	}
	rf.HeartbeatTimer.Reset(time.Duration(500) * time.Millisecond)
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
	rf.IsLeader = false
	rf.ElectionTimer = time.AfterFunc(time.Duration(2+int(4*rand.Float32()))*time.Second, rf.sendAllVotes)
	rf.HeartbeatTimer = time.AfterFunc(time.Duration(500)*time.Millisecond, rf.sendAllAppendEntries)
	rf.VotedFor = -1
	rf.CurrentTerm = 0
	fmt.Printf("rf: %+v\n", rf)
	// Your initialization code here (2A, 2B, 2C).
	// AQUI: Modify Make() to
	// create a background goroutine that will kick off leader
	// election periodically by sending out RequestVote RPCs when it hasn't
	// heard from another peer for a while. This way a peer will learn
	// who is the leader, if there is already leader,
	// or become itself the leader.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

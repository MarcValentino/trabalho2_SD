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
	"math/rand"
	"sync"
	"time"

	"github.com/MarcValentino/trabalho2_SD/src/labrpc"
)

const HEARTBEAT_TIMEOUT = 100
const ELECTION_TIMEOUT = 300
const TIMEOUT_RANGE = 300

const FOLLOWER = 0
const CANDIDATE = 1
const LEADER = 2

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
	mu                 sync.Mutex          // Lock to protect shared access to this peer's state
	peers              []*labrpc.ClientEnd // RPC end points of all peers
	persister          *Persister          // Object to hold this peer's persisted state
	me                 int                 // this peer's index into peers[]
	CurrentTerm        int
	VotedFor           int
	State              int
	changeStateChannel chan bool
	electionWinChannel chan bool
	voteCount          int
}

// pergunta: o que isso quer dizer?
func (rf *Raft) GetState() (int, bool) {

	// Your code here.
	rf.mu.Lock()
	term := rf.CurrentTerm
	isleader := false
	if rf.State == LEADER {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	Entries  bool
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()

	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		reply.Success = false
	} else if args.Term > rf.CurrentTerm {
		rf.State = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.LeaderId
		rf.changeStateChannel <- true
	} else {
		rf.State = FOLLOWER
		rf.VotedFor = args.LeaderId
		rf.changeStateChannel <- true
	}
	rf.mu.Unlock()

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm

	if args.Term > rf.CurrentTerm || (args.Term == rf.CurrentTerm && rf.VotedFor == -1) {
		rf.State = FOLLOWER
		rf.CurrentTerm = args.Term
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		rf.changeStateChannel <- true
	}

	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func getTimeout() time.Duration {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	return time.Duration(r1.Intn(TIMEOUT_RANGE) + ELECTION_TIMEOUT)
}

func (rf *Raft) makeElection() {
	args := RequestVoteArgs{}

	rf.mu.Lock()
	args.Term = rf.CurrentTerm
	args.CandidateId = rf.me
	rf.voteCount = 0
	rf.VotedFor = rf.me
	rf.mu.Unlock()

	major_votes := len(rf.peers)/2 + 1

	for k := 0; k < len(rf.peers); k++ {
		if k == rf.me {
			rf.mu.Lock()
			rf.voteCount++
			rf.mu.Unlock()
			continue
		}
		// Manda o sendRequestVote em paralelo para os demais servers.
		go func(server int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				if rf.State == CANDIDATE {
					if reply.VoteGranted {
						rf.voteCount++
						if rf.voteCount >= major_votes {
							rf.State = LEADER
							rf.electionWinChannel <- true
						}
					}
				}
				// Se o termo do rpc for maior que o termo atual do rf ele vira seguidor
				if reply.Term > rf.CurrentTerm {
					rf.State = FOLLOWER
					rf.CurrentTerm = args.Term
					rf.VotedFor = -1

					rf.changeStateChannel <- true
				}
				rf.mu.Unlock()
			}
		}(k)
	}
}

func (rf *Raft) heartbeat() {

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		server := i
		args := &AppendEntriesArgs{}
		args.Entries = false
		args.Term = rf.CurrentTerm
		args.LeaderId = rf.me
		rf.mu.Unlock()

		go func() {
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(server, args, &reply)

			rf.mu.Lock()
			// Se receber uma resposta com Term > currentTerm muda para FOLLOWER
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1
				rf.State = FOLLOWER

				rf.changeStateChannel <- true

			}
			rf.mu.Unlock()

		}()
	}
}

func (rf *Raft) loopTrue() {
	var timeout time.Duration
	for true {
		switch rf.State {
		case FOLLOWER:

		forTrueFollower:
			for true {
				timeout = getTimeout()
				select {
				case <-time.After(timeout * time.Millisecond):
					rf.mu.Lock()
					rf.State = CANDIDATE
					rf.CurrentTerm++
					rf.mu.Unlock()
					break forTrueFollower
				case <-rf.changeStateChannel:
				}
			}
		case CANDIDATE:

		forTrueCandidate:
			for true {
				rf.makeElection()

				timeout = getTimeout()

				select {
				case <-time.After(timeout * time.Millisecond):
					rf.mu.Lock()
					rf.CurrentTerm++
					rf.mu.Unlock()
				case <-rf.electionWinChannel:
					break forTrueCandidate
				case <-rf.changeStateChannel:
					break forTrueCandidate
				}
			}
		case LEADER:

		forTrueLeader:
			for true {
				rf.heartbeat()
				select {
				case <-time.After(HEARTBEAT_TIMEOUT * time.Millisecond):
				case <-rf.changeStateChannel:
					break forTrueLeader
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
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.State = FOLLOWER
	rf.voteCount = 0

	rf.changeStateChannel = make(chan bool)
	rf.electionWinChannel = make(chan bool)

	go rf.loopTrue()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

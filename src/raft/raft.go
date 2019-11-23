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
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

type raftStatus int
const (
	follower raftStatus = 0
	candidate       = 1
	leader          = 2
)

type raftLog struct {
	index 	int
	term	int
	command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	elecMu	sync.Mutex

	currentTerm int
	votedFor 	int
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	leaderId    int

	// leader, follower or candidate
	status raftStatus

	// logs
	logs []raftLog

	// timer
	timer *time.Timer

	// chan for peer request
	peerRequestChan chan int

	// chan for election
	elecChan chan int

	// chan for leader
	leaderChan chan int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.status == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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
	// Your data here (2A, 2B).
	term int
	candidateId int
	lastLogIndex int
	lastLogTerm int
	serverId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term int
	voteGranted bool
	serverId int
}

type AppendEntriesArgs struct {
	term 	int
	leaderId int
	prevLogIndex int
	prevLogTerm int
	leaderCommit int
	entries []raftLog
	serverId int
}

type AppendEntriesReply struct {
	serverId int
	term int
	success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	if args.term > rf.currentTerm {
		// candidate's term is higher, update rf's term
		rf.meetHigherTerm(args.term)
	}
	reply.serverId = rf.me
	reply.term = rf.currentTerm

	if args.term < rf.currentTerm {
		// candidate's term is lower, reject vote
		reply.voteGranted = false
	} else if args.term == rf.currentTerm {
		// candidate's and current rf's terms are the same
		if rf.votedFor == -1 {
			// vote not granted yet, check the up-to-date logs
			lastLogIndex := -1
			lastLogTerm := -1
			if len(rf.logs) > 0 {
				lastLogIndex = rf.logs[len(rf.logs) - 1].index
				lastLogTerm = rf.logs[len(rf.logs) - 1].term
			}

			reply.voteGranted = false
			if lastLogTerm < args.lastLogTerm {
				// candidate's last log is in a higher term, grant vote
				rf.resetTimer()
				reply.voteGranted = true
			} else if lastLogTerm == args.lastLogTerm && lastLogIndex <= args.lastLogIndex {
				// candidate's and current rf's last logs are in the same term, and candidate's log is longer, grant vote
				rf.resetTimer()
				reply.voteGranted = true
			}
		} else {
			// vote granted, reject vote
			reply.voteGranted = false
		}
	} else {
		// print some log
	}

	rf.mu.Unlock()
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	if args.term > rf.currentTerm {
		// candidate's term is higher, update rf's term
		rf.meetHigherTerm(args.term)
	}
	reply.serverId = rf.me
	reply.term = rf.currentTerm

	if args.term < rf.currentTerm {
		// candidate's term is lower, reject vote
		reply.success = false
	} else {
		reply.success = true
	}

	rf.mu.Unlock()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, requstVoteChan chan int, replyVoteChan chan int) {
	i := 0
	ok := false

	// try for 10 times until the request is processed by the callee
	for {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		i += 1
		select {
		case <- requstVoteChan:
			break
		default:
			if ok || i >= 10 {
				break
			}
		}
	}

	if ok {
		rf.mu.Lock()
		// the callee's term is higher, update term
		if reply.term > rf.currentTerm {
			rf.meetHigherTerm(reply.term)
		}
		rf.mu.Unlock()

		if reply.voteGranted {
			replyVoteChan <- 1
		}else {
			replyVoteChan <- 0
		}
	}
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendEntriesChan chan int, replyAppendChan chan int){
	ok := false
	for {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		select {
		case <- appendEntriesChan:
			break
		default:
			if ok {
				break
			}
		}
	}

	rf.mu.Lock()
	if ok {
		if rf.currentTerm < reply.term {
			rf.meetHigherTerm(reply.term)
		}
		if reply.success {
			replyAppendChan <- 1
		} else {
			replyAppendChan <- 0
		}
	}
	rf.mu.Unlock()
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied  = 0
	rf.status = follower
	rf.logs = make([]raftLog, 0)
	rf.peerRequestChan = make(chan int)
	rf.elecChan = make(chan int)
	elapseTime := time.Millisecond * time.Duration(rand.Intn(150) + 150)
	rf.timer = time.NewTimer(elapseTime)

	go func(){
		for {
			select {
			case <- rf.timer.C:
				rf.mu.Lock()
				if rf.status == candidate {
					// stop election
					rf.elecChan <- 1
					// start new term
					rf.resetTerm(rf.currentTerm + 1)
					// restart election
					go rf.startElection(rf.currentTerm)
				} else if rf.status == follower {
					// start election
					rf.status = candidate
					// start new term
					rf.resetTerm(rf.currentTerm + 1)
					// start election
					go rf.startElection(rf.currentTerm)
				}

				// reset the timer
				rf.resetTimer()
				rf.mu.Unlock()
			}
		}

	} ()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) startElection(elecTerm int){

	rf.elecMu.Lock()
	defer rf.elecMu.Unlock()

	replyVoteChan := make(chan int)
	votesReceived := 0

	rf.mu.Lock()
	requestVoteChans := make([]chan int, len(rf.peers))

	// vote for self
	rf.votedFor = rf.me
	votesReceived += 1

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		voteRequest := &RequestVoteArgs{elecTerm,
			rf.me,
			rf.logs[len(rf.logs) - 1].index,
			rf.logs[len(rf.logs) - 1].term,
			i}
		voteRequestReply := &RequestVoteReply{}
		go rf.sendRequestVote(i, voteRequest, voteRequestReply, requestVoteChans[i], replyVoteChan)
	}
	rf.mu.Unlock()

	for {
		select {
		case <- rf.elecChan:
			// the election is stopped, inform all sendVoteRequest to stop
			for _, c := range requestVoteChans {
				c <- 1
			}
			break
		case voteGet := <- replyVoteChan:
			if voteGet == 1 {
				votesReceived += 1
			}

			if voteGet > len(rf.peers) / 2 {
				// get more than half of votes, become leader
				rf.mu.Lock()

				// make sure the election process is not an stale process
				if elecTerm != rf.currentTerm || rf.status != candidate {
					break
				}
				rf.status = leader
				rf.mu.Unlock()

				// initialize leader's data structure
				// send heartbeats
				go rf.startSendHeartBeat()

				// the election is stopped, inform all sendVoteRequest to stop
				for _, c := range requestVoteChans {
					c <- 1
				}
				break
			}
		}
	}
}

func (rf *Raft) startApendEntries(isHeartBeat bool){

	replyChan := make(chan int)
	//agreeReceived := 0

	rf.mu.Lock()
	appendEntriesChans := make([]chan int, len(rf.peers))
	prevLogIndex := -1
	prevLogTerm := -1
	if len(rf.logs) > 0 {
		prevLogIndex = rf.logs[len(rf.logs)].index
		prevLogTerm = rf.logs[len(rf.logs)].term
	}

	for i, _ := range rf.peers {
		if isHeartBeat {
			args := &AppendEntriesArgs{rf.currentTerm,
				rf.me,
				prevLogIndex,
				prevLogTerm,
				rf.commitIndex,
				nil,
				i}
			reply := &AppendEntriesReply{}
			go rf.sendAppendEntries(i, args, reply, appendEntriesChans[i], replyChan)
		}
	}

	rf.mu.Unlock()
}

func (rf *Raft) startSendHeartBeat(){
	interval := 75 * time.Millisecond
	ticker := time.Tick(interval)
	for {
		select {
		case <- rf.leaderChan:
			break
		case <- ticker:
			go rf.startApendEntries(true)
		}
	}
}

// woork under mutex
func (rf *Raft) resetTerm(newTerm int){
	rf.currentTerm = newTerm
	rf.votedFor = -1
}

// work under mutex
func (rf *Raft) resetTimer(){
	elapseTime := time.Millisecond * time.Duration(rand.Intn(150) + 150)
	rf.timer.Reset(elapseTime)
}

// work under mutex
func (rf *Raft) meetHigherTerm(newTerm int){
	if rf.status == candidate {
		// stop election & switch to follower
		rf.elecChan <- 1
		rf.status = follower
	} else if rf.status == leader {
		// switch to follower
		rf.leaderChan <- 1
		rf.status = follower
	}
	rf.resetTerm(newTerm)
}

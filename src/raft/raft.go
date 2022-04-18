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
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//

type elecState int // state for leader election

const (
	FOLLOWER elecState = iota
	CANDADITE
	LEADER
)

type elecTimer struct {
	mu               sync.Mutex // for elapsed
	elapsed, timeout int        // unit: ms
}

func (et *elecTimer) reset() {
	et.mu.Lock()
	defer et.mu.Unlock()
	et.elapsed = 0
}

func (et *elecTimer) add() {
	et.mu.Lock()
	defer et.mu.Unlock()
	et.elapsed++
}

func (et *elecTimer) timedout() bool {
	et.mu.Lock()
	defer et.mu.Unlock()
	return et.elapsed >= et.timeout
}

type voteInfo struct {
	votedFor  int        // candidateId which this server voted for in current term, -1 means haven't voted yet
	votedTerm int        // the last term votedFor
	beVoted   int        // the number of votes received
	muVoting  sync.Mutex // for votedTerm, i.e. the whole vote process
	muVoted   sync.Mutex // for beVoted
}

func (vi *voteInfo) initVotes() {
	vi.muVoted.Lock()
	defer vi.muVoted.Unlock()
	vi.beVoted = 1 // vote for self
}

func (vi *voteInfo) holdVotes() int {
	vi.muVoted.Lock()
	defer vi.muVoted.Unlock()
	return vi.beVoted
}

func (vi *voteInfo) addVotes() {
	vi.muVoted.Lock()
	defer vi.muVoted.Unlock()
	vi.beVoted++
}

type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	muState     sync.Mutex // Lock to protect shared access to this peer's state
	muTerm      sync.Mutex // for currentTerm
	muElect     sync.Mutex // // for electing
	es          elecState  // current election state
	currentTerm int        // current term
	electing    bool       // whether the election going on
	vi          voteInfo
	et          elecTimer
}

func (rf *Raft) getTerm() int {
	rf.muTerm.Lock()
	defer rf.muTerm.Unlock()
	return rf.currentTerm
}

func (rf *Raft) addTerm() {
	rf.muTerm.Lock()
	defer rf.muTerm.Unlock()
	rf.currentTerm++
}

func (rf *Raft) updateTerm(a int) {
	rf.muTerm.Lock()
	defer rf.muTerm.Unlock()
	rf.currentTerm = a
}

func (rf *Raft) changeState(s elecState) {
	rf.muState.Lock()
	defer rf.muState.Unlock()
	rf.es = s
}

func (rf *Raft) isState(s elecState) bool {
	rf.muState.Lock()
	defer rf.muState.Unlock()
	return rf.es == s
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// // Your code here (2A).
	// return term, isleader
	return rf.getTerm(), rf.isState(LEADER)
}

func (rf *Raft) inElection() bool {
	rf.muElect.Lock()
	defer rf.muElect.Unlock()
	return rf.electing
}

func (rf *Raft) changeElection(a bool) {
	rf.muElect.Lock()
	defer rf.muElect.Unlock()
	rf.electing = a
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

func (rf *Raft) kickOffElection() {
	// transform state to condidate, and kick off a new election
	rf.et.reset()
	rf.changeState(CANDADITE)
	rf.addTerm()
	rf.vi.initVotes()
	rf.changeElection(true)
	rvArgs := RequestVoteArgs{rf.getTerm(), rf.me}
	for i := 0; i < len(rf.peers) && rf.inElection(); i++ {
		if i != rf.me {
			go rf.sendAndReceiveRVRPC(i, &rvArgs)
		}
	}
}

func (rf *Raft) sendAndReceiveRVRPC(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, reply)
	if !ok {
		// fmt.Println("sendRequestVote() error")
		return
	}

	if reply.VoteGranted {
		rf.vi.addVotes()
		if rf.inElection() && rf.vi.holdVotes() >= len(rf.peers)/2+1 { // received votes from the majority of servers
			rf.changeState(LEADER)
			rf.et.reset()
			rf.changeElection(false)
			go rf.heartbeat()
		}
	} else if reply.Term > rf.currentTerm {
		rf.changeState(FOLLOWER)
		rf.updateTerm(reply.Term)
		rf.changeElection(false)
	}
}

func (rf *Raft) sendAndReceiveAERPC(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		// fmt.Println("sendAppendEntries() error")
		return
	}
	if !reply.Success && rf.getTerm() < reply.Term {
		rf.changeState(FOLLOWER)
		rf.updateTerm(reply.Term)
	}
}

func (rf *Raft) heartbeat() {
	for _, s := rf.GetState(); s; _, s = rf.GetState() {
		args := AppendEntriesArgs{}
		args.LeaderId = rf.me
		args.Term = rf.currentTerm
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.sendAndReceiveAERPC(i, &args)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) timeout() {
	for {
		if _, s := rf.GetState(); !s { // this server believes it is the leader
			if !rf.et.timedout() {
				time.Sleep(time.Millisecond)
				rf.et.add()
				continue
			}
			go rf.kickOffElection()
		}
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.vi.muVoting.Lock()
	defer rf.vi.muVoting.Unlock()
	if rf.vi.votedTerm < args.Term && // have not voted in the current term
		((rf.isState(FOLLOWER) && rf.getTerm() <= args.Term) ||
			(!rf.isState(FOLLOWER) && rf.getTerm() < args.Term)) {
		rf.changeState(FOLLOWER)
		rf.updateTerm(args.Term)
		rf.vi.votedFor = args.CandidateId
		rf.vi.votedTerm = args.Term
		rf.et.reset()
		reply.VoteGranted = true
	}
	reply.Term = rf.getTerm()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.getTerm()
	if args.Term >= rf.getTerm() {
		rf.changeState(FOLLOWER)
		rf.updateTerm(args.Term)
		rf.changeElection(false)
		rf.et.reset()
	}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	// initialize vote info
	rf.vi.votedFor = -1
	rf.vi.votedTerm = -1

	// initialize timer info
	rf.et.timeout = rand.Int()%200 + 300 // [300, 500) ms

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.timeout()

	return rf
}

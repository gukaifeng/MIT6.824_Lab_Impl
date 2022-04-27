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
	votedFor   int        // candidateId which this server voted for in current term, -1 means haven't voted yet
	beVoted    int        // the number of votes received
	muVoting   sync.Mutex // for the whole vote process
	muVotedFor sync.Mutex
	muVoted    sync.Mutex // for beVoted
}

func (vi *voteInfo) setVotedFor(a int) {
	vi.muVotedFor.Lock()
	defer vi.muVotedFor.Unlock()
	vi.votedFor = a
}
func (vi *voteInfo) getVotedFor() int {
	vi.muVotedFor.Lock()
	defer vi.muVotedFor.Unlock()
	return vi.votedFor
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

type logEntry struct {
	Entry interface{}
	Term  int
}

type replicateCount struct {
	count int
	mu    sync.Mutex
}

func (rc *replicateCount) addCount() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.count++
}

func (rc *replicateCount) getCount() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.count
}

type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	muState sync.Mutex // Lock to protect shared access to this peer's state
	es      elecState  // current election state

	muTerm      sync.Mutex // for currentTerm
	currentTerm int        // current term

	vi         voteInfo
	muBeLeader sync.Mutex // for the process of being leader
	et         elecTimer

	log   []logEntry
	muLog sync.Mutex // for append and start agreememt in the leader

	nextIndex    []int
	matchIndex   []int
	muAgreements []sync.Mutex

	commitMu    sync.Mutex
	commitIndex int

	muApply     sync.Mutex
	lastApplied int

	applyCh chan ApplyMsg
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      interface{}
	EntriesT     int
	Term         int // the entries in the same RPC have the same term
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return: the index of the new log entry
func (rf *Raft) appendLog(le logEntry) int {
	rf.muLog.Lock()
	defer rf.muLog.Unlock()
	rf.log = append(rf.log, le)
	return len(rf.log) - 1
}

func (rf *Raft) getLogLen() int {
	rf.muLog.Lock()
	defer rf.muLog.Unlock()
	return len(rf.log)
}

func (rf *Raft) readLog(i int) (interface{}, int) {
	rf.muLog.Lock()
	defer rf.muLog.Unlock()
	return rf.log[i].Entry, rf.log[i].Term
}

func (rf *Raft) trimRightLog(end int) {
	rf.muLog.Lock()
	defer rf.muLog.Unlock()
	rf.log = rf.log[:end]
}

func (rf *Raft) getCommitIndex() int {
	rf.commitMu.Lock()
	defer rf.commitMu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) setCommitIndex(a int) {
	rf.commitMu.Lock()
	defer rf.commitMu.Unlock()
	rf.commitIndex = a
}

func (rf *Raft) getCurrentTerm() int {
	rf.muTerm.Lock()
	defer rf.muTerm.Unlock()
	return rf.currentTerm
}

func (rf *Raft) increaseCurrentTerm() {
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
	return rf.getCurrentTerm(), rf.isState(LEADER)
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

func (rf *Raft) startElection() {
	rf.increaseCurrentTerm()
	rf.changeState(CANDADITE)

	rvArgs := RequestVoteArgs{}
	rvArgs.CandidateId = rf.me
	rvArgs.Term = rf.getCurrentTerm()
	rvArgs.LastLogIndex = rf.getLogLen() - 1
	_, rvArgs.LastLogTerm = rf.readLog(rvArgs.LastLogIndex)

	rf.vi.muVoted.Lock()
	rf.vi.beVoted = 1 // vote for self
	rf.vi.setVotedFor(rf.me)
	rf.vi.muVoted.Unlock()

	DPrintf("候选者 %v 发起了任期 %v 的选举，当前 %v，当前服务器数 %v\n", rf.me, rf.getCurrentTerm(), rf.isState(LEADER), len(rf.peers))
	for i := 0; i < len(rf.peers) && rf.isState(CANDADITE); i++ {
		if i != rf.me {
			go rf.startRequestVote(i, &rvArgs)
		}
	}
}

func (rf *Raft) startRequestVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}

	ok := rf.sendRequestVote(server, args, reply)
	for i := 0; !ok && rf.isState(CANDADITE) && i < 5; i++ {
		time.Sleep(5 * time.Millisecond)
		ok = rf.sendRequestVote(server, args, reply)
	}
	if !(ok && rf.isState(CANDADITE)) {
		return
	}

	// rf.et.reset()

	if reply.Term > rf.getCurrentTerm() {
		rf.changeState(FOLLOWER)
		rf.updateTerm(reply.Term)
		// rf.vi.votedFor = -1
		rf.vi.setVotedFor(-1)
		return
	}

	if reply.VoteGranted {
		rf.vi.addVotes()

		rf.muBeLeader.Lock()
		defer rf.muBeLeader.Unlock()
		if rf.isState(CANDADITE) && rf.vi.holdVotes() >= len(rf.peers)/2+1 { // received votes from the majority of servers
			DPrintf("服务器 %v 成为了任期 %v 的领导者\n", rf.me, rf.getCurrentTerm())

			rf.changeState(LEADER)

			nServers := len(rf.peers)
			lenLog := rf.getLogLen()
			for i := 0; i < nServers; i++ {
				rf.nextIndex[i] = lenLog
				rf.matchIndex[i] = 0
			}

			go rf.startHeartbeat()
		}
	}
}

// func (rf *Raft) sendAndReceiveAERPC(server int, args *AppendEntriesArgs, rc *replicateCount) {
func (rf *Raft) sendReplication(server int, index int, rc *replicateCount) {
	rf.muAgreements[server].Lock()
	defer rf.muAgreements[server].Unlock()

	if !rf.isState(LEADER) || index > rf.getLogLen()-1 {
		return
	}

	_, entryTerm := rf.readLog(index)

	args := &AppendEntriesArgs{}
	args.LeaderId = rf.me
	args.Term = rf.getCurrentTerm()
	args.LeaderCommit = rf.getCommitIndex()

	for rf.isState(LEADER) && rf.nextIndex[server] <= index {
		args.PrevLogIndex = rf.nextIndex[server] - 1
		_, args.PrevLogTerm = rf.readLog(args.PrevLogIndex)
		args.Entries, args.EntriesT = rf.readLog(rf.nextIndex[server])
		reply := &AppendEntriesReply{}

		// DPrintf("领导者 %v 正在给追随者 %v 发送索引 %v 处的条目 %v，当前任期 %v\n", rf.me, server, rf.nextIndex[server], args.Entries, rf.getCurrentTerm())

		ok := rf.sendAppendEntries(server, args, reply)
		for !ok && rf.isState(LEADER) {
			time.Sleep(5 * time.Millisecond)
			ok = rf.sendAppendEntries(server, args, reply)
		}
		if !(ok && rf.isState(LEADER)) {
			return
		}

		if !reply.Success {
			if reply.Term > args.Term {
				rf.changeState(FOLLOWER)
				rf.updateTerm(reply.Term)
				rf.vi.setVotedFor(-1)
				return
			}
			rf.nextIndex[server]--
			DPrintf("领导者 %v 减小了追随者 %v 的 nextIndex，当前 nextIndex = %v，当前任期 %v\n", rf.me, server, rf.nextIndex[server], rf.getCurrentTerm())
		} else {
			rf.matchIndex[server] = rf.nextIndex[server]
			rf.nextIndex[server]++
		}
	}
	if rf.isState(LEADER) && rf.getCurrentTerm() == entryTerm {
		rc.addCount()
	}
}

func (rf *Raft) startHeartbeat() {
	for rf.isState(LEADER) {
		// DPrintf("领导者 %v 发起了一轮心跳，其任期为 %v\n", rf.me, rf.getCurrentTerm())
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.sendHeartbeat(i)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeat(server int) {

	args := &AppendEntriesArgs{}
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.getLogLen() - 1
	_, args.PrevLogTerm = rf.readLog(args.PrevLogIndex)
	args.LeaderCommit = rf.getCommitIndex() // it must be less than or equal to PrevLogIndex
	args.Term = rf.getCurrentTerm()

	reply := &AppendEntriesReply{}

	ok := rf.sendAppendEntries(server, args, reply)
	for !ok && rf.isState(LEADER) {
		time.Sleep(5 * time.Millisecond)
		ok = rf.sendAppendEntries(server, args, reply)
	}
	if !(ok && rf.isState(LEADER)) {
		return
	}

	if !reply.Success && reply.Term > rf.getCurrentTerm() {
		rf.changeState(FOLLOWER)
		rf.updateTerm(reply.Term)
		rf.vi.setVotedFor(-1)
		return
	}
}

func (rf *Raft) electionTimer() {
	for {
		if !rf.isState(LEADER) {
			if !rf.et.timedout() {
				time.Sleep(time.Millisecond)
				rf.et.add()
				continue
			}
			rf.et.reset()
			go rf.startElection()
		}
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.getCurrentTerm()

	if args.Term < rf.getCurrentTerm() {
		return
	}

	// if rf.vi.getVotedFor() != -1 && args.Term > rf.getCurrentTerm() {
	if args.Term > rf.getCurrentTerm() {
		rf.changeState(FOLLOWER)
		rf.updateTerm(args.Term)
		rf.vi.setVotedFor(-1)
	}

	lastLogIndex := rf.getLogLen() - 1
	_, lastLogTerm := rf.readLog(lastLogIndex)
	if args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		return
	}

	// DPrintf("候选人 %v 请求服务器 %v 投票，任期 %v，服务器总数 %v\n", args.CandidateId, rf.me, args.Term, len(rf.peers))
	rf.vi.muVoting.Lock()
	defer rf.vi.muVoting.Unlock()
	// DPrintf("候选人 %v 请求服务器 %v 投票，%v %v\n", args.CandidateId, rf.me, rf.vi.votedTerm, args.Term)
	// DPrintf("%v %v\n", args.Term, rf.vi.votedTerm)
	// if args.Term > rf.vi.votedTerm {
	if rf.vi.getVotedFor() == -1 {
		// DPrintf("服务器 %v 给服务器 %v 投了一票\n", rf.me, args.CandidateId)
		rf.et.reset()
		rf.changeState(FOLLOWER)
		rf.updateTerm(args.Term)
		rf.vi.setVotedFor(args.CandidateId)
		reply.VoteGranted = true
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	currTerm := rf.getCurrentTerm()
	reply.Term = currTerm

	if args.Term < currTerm {
		return
	}

	rf.et.reset()

	if args.Term > currTerm {
		rf.updateTerm(args.Term)
	}

	if !rf.isState(FOLLOWER) {
		rf.changeState(FOLLOWER)
		rf.vi.setVotedFor(args.LeaderId)
	}

	lastLogIndex := rf.getLogLen() - 1
	_, lastLogTerm := rf.readLog(lastLogIndex)
	if lastLogIndex < args.PrevLogIndex ||
		(lastLogIndex == args.PrevLogIndex && lastLogTerm != args.PrevLogTerm) {
		return
	}

	if args.Entries != nil {
		if lastLogIndex > args.PrevLogIndex {
			nextLogIndex := args.PrevLogIndex + 1
			_, nextLogTerm := rf.readLog(nextLogIndex)
			if nextLogTerm == args.EntriesT { // the follower already has the next entry to append
				reply.Success = true
				return
			} else {
				// truncate
				DPrintf("截断")
				rf.trimRightLog(nextLogIndex)
				lastLogIndex = rf.getLogLen() - 1
				_, lastLogTerm = rf.readLog(lastLogIndex)
			}
		}
		if lastLogIndex == args.PrevLogIndex &&
			lastLogTerm == args.PrevLogTerm {
			rf.appendLog(logEntry{args.Entries, args.EntriesT})
			DPrintf("追随者 %v 在索引 %v 处追加了 %v，当前日志长度 %v，当前任期 %v\n",
				rf.me, rf.getLogLen()-1, args.Entries, rf.getLogLen(), args.Term)
			reply.Success = true

		}
	} else if lastLogIndex == args.PrevLogIndex &&
		lastLogTerm == args.PrevLogTerm {
		reply.Success = true
	}

	if reply.Success {
		rf.setCommitIndex(Min(args.LeaderCommit, rf.getLogLen()-1))
	}

	// reply.Success = true
	go rf.apply()
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
// if you're having troub:le getting RPC to work, check that you've
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

func (rf *Raft) apply() {
	rf.muApply.Lock()
	defer rf.muApply.Unlock()
	for ; rf.lastApplied < rf.getCommitIndex(); rf.lastApplied++ {
		// DPrintf("== %v %v ==", rf.lastApplied, rf.commitIndex)
		e, _ := rf.readLog(rf.lastApplied + 1)
		rf.applyCh <- ApplyMsg{true, e, rf.lastApplied + 1}
		if rf.isState(LEADER) {
			DPrintf("领导者 ")
		} else {
			DPrintf("追随者 ")
		}
		DPrintf("%v 应用了索引 %v 处的 %v，当前日志长度 %v，当前 commitIndex 为 %v，当前任期 %v\n", rf.me, rf.lastApplied+1, e, rf.getLogLen(), rf.getCommitIndex(), rf.getCurrentTerm())
	}
}

func (rf *Raft) startAgreement(index int, command interface{}, rc *replicateCount) {

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendReplication(i, index, rc)
		}
	}

	for rc.getCount() < len(rf.peers)/2+1 {
		time.Sleep(5 * time.Millisecond)
	}

	if index > rf.getCommitIndex() {
		rf.setCommitIndex(index)
	}

	go rf.apply()
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	// Your code here (2B).
	if rf.isState(LEADER) {
		rc := replicateCount{}
		rc.addCount()
		index1 := rf.appendLog(logEntry{command, rf.getCurrentTerm()})
		DPrintf("领导者 %v 在索引 %v 处追加了 %v，当前任期 %v\n", rf.me, index1, command, rf.getCurrentTerm())
		go rf.startAgreement(index1, command, &rc)

		index = index1
		term = rf.getCurrentTerm()
		isLeader = true
	}
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

	// initialize timer info
	rf.et.timeout = rand.Int()%150 + 150 // [150, 300) ms

	rf.log = append(rf.log, logEntry{0, 0}) // initial consistency, the lock is unnecessary

	nServers := len(peers)
	rf.nextIndex = make([]int, nServers)
	rf.matchIndex = make([]int, nServers)
	rf.muAgreements = make([]sync.Mutex, nServers)

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// DPrintf("服务器 %v 开始超时计时，当前任期 %v，服务器数量 %v\n", rf.me, rf.getCurrentTerm(), len(peers))
	go rf.electionTimer()

	return rf
}

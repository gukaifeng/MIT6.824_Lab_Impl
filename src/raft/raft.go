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

type logEntry struct {
	Entry interface{}
	Term  int
	// Count     int
	// Mu        sync.Mutex // for Count
	// Committed bool
}

// func (e *logEntry) addCount() {
// 	e.Mu.Lock()
// 	defer e.Mu.Unlock()
// 	e.Count++
// }

// func (e *logEntry) getCount() int {
// 	e.Mu.Lock()
// 	defer e.Mu.Unlock()
// 	return e.Count
// }

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

	muElect  sync.Mutex // for electing
	electing bool       // whether the election going on

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

	lastApplied int

	applyCh chan ApplyMsg
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

func (rf *Raft) startElection() {
	// transform state to condidate, and kick off a new election
	// rf.et.reset()

	rvArgs := RequestVoteArgs{}
	rvArgs.CandidateId = rf.me
	rvArgs.Term = rf.getTerm()
	rvArgs.LastLogIndex = rf.getLogLen() - 1
	_, rvArgs.LastLogTerm = rf.readLog(rvArgs.LastLogIndex)
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
	// rf.et.reset()

	if reply.VoteGranted {
		rf.vi.addVotes()

		rf.muBeLeader.Lock()
		defer rf.muBeLeader.Unlock()
		if rf.inElection() && rf.vi.holdVotes() >= len(rf.peers)/2+1 { // received votes from the majority of servers
			// fmt.Printf("服务器 %v 成为了任期 %v 的领导者\n", rf.me, rf.getTerm())
			rf.changeElection(false)
			rf.changeState(LEADER)
			rf.et.reset()

			nServers := len(rf.peers)
			nLogEntries := rf.getLogLen()
			rf.nextIndex = make([]int, nServers)
			rf.matchIndex = make([]int, nServers)
			rf.muAgreements = make([]sync.Mutex, nServers)
			for i := 0; i < nServers; i++ {
				rf.nextIndex[i] = nLogEntries
			}

			go rf.heartbeat()
		}
	} else if reply.Term > rf.getTerm() {
		rf.changeState(FOLLOWER)
		rf.updateTerm(reply.Term)
		rf.changeElection(false)
	}
}

// func (rf *Raft) sendAndReceiveAERPC(server int, args *AppendEntriesArgs, rc *replicateCount) {
func (rf *Raft) sendReplication(server int, rc *replicateCount) {
	rf.muAgreements[server].Lock()
	defer rf.muAgreements[server].Unlock()
	for rf.isState(LEADER) && rf.nextIndex[server] < rf.getLogLen() {
		args := &AppendEntriesArgs{}
		args.LeaderCommit = rf.getCommitIndex()
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[server] - 1
		_, args.PrevLogTerm = rf.readLog(args.PrevLogIndex)
		args.Entries, args.Term = rf.readLog(rf.nextIndex[server])
		// fmt.Printf("领导者 %v 正在给追随者 %v 发送索引为 %v 的条目 %v， 当前任期 %v\n", rf.me, server, rf.nextIndex[server], args.Entries, rf.getTerm())

		reply := &AppendEntriesReply{}

		ok := rf.sendAppendEntries(server, args, reply)
		if !ok {
			continue // retry
		}

		if reply.Success {
			rf.matchIndex[server] = rf.nextIndex[server]
			rf.nextIndex[server]++
		} else {
			if rf.getTerm() < reply.Term {
				// fmt.Printf("领导者 %v 卸任，返回追随者。原任期 %v，收到任期 %v\n", rf.me, rf.getTerm(), reply.Term)
				rf.changeState(FOLLOWER)
				rf.updateTerm(reply.Term)
				return
			}
			rf.nextIndex[server]--
			// fmt.Printf("任期 %v 的领导者 %v 减小了追随者 %v 的 nextIndex，当前 nextIndex = %v\n", rf.getTerm(), rf.me, server, rf.nextIndex[server])
			// fmt.Printf("%v %v %v\n", rf.isState(LEADER), rf.nextIndex[server], len(rf.log))
		}
	}
	rc.addCount()
}

func (rf *Raft) heartbeat() {
	for rf.isState(LEADER) {
		// fmt.Printf("领导者 %v 发起了一轮心跳，其任期为 %v\n", rf.me, rf.getTerm())
		args := AppendEntriesArgs{}
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.getLogLen() - 1
		_, args.PrevLogTerm = rf.readLog(args.PrevLogIndex)
		args.LeaderCommit = rf.getCommitIndex() // it must be less than or equal to PrevLogIndex
		args.Term = rf.getTerm()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.sendHeartbeat(i, &args)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeat(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if ok && !reply.Success {
		// fmt.Printf("领导者 %v 转为了追随者，任期由 %v 修改为 %v\n", rf.me, rf.getTerm(), reply.Term)
		rf.changeState(FOLLOWER)
		rf.updateTerm(reply.Term)
	}
}

func (rf *Raft) timeout() {
	for {
		if !rf.isState(LEADER) {
			if !rf.et.timedout() {
				time.Sleep(time.Millisecond)
				rf.et.add()
				continue
			}
			// fmt.Printf("服务器 %v 超时\n", rf.me)
			// fmt.Printf("候选者 %v 发起了任期 %v 的选举，当前 %v\n", rf.me, rf.getTerm(), rf.isState(LEADER))
			rf.et.reset()
			rf.changeState(CANDADITE)
			rf.addTerm()
			rf.vi.initVotes()
			rf.changeElection(true)
			// fmt.Printf("候选者 %v 发起了任期 %v 的选举，当前其是不是领导者 %v\n", rf.me, rf.getTerm(), rf.isState(LEADER))
			go rf.startElection()
		}
	}
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
	Term         int // the entries in the same RPC have the same term
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
	lastLogIndex := rf.getLogLen() - 1
	_, lastLogTerm := rf.readLog(lastLogIndex)

	rf.vi.muVoting.Lock()
	defer rf.vi.muVoting.Unlock()
	if rf.vi.votedTerm < args.Term && // have not voted in the current term
		((rf.isState(FOLLOWER) && rf.getTerm() <= args.Term) ||
			(!rf.isState(FOLLOWER) && rf.getTerm() < args.Term)) &&
		(args.LastLogTerm >= lastLogTerm && args.LastLogIndex >= lastLogIndex) {
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
	lastLogIndex := rf.getLogLen() - 1
	_, lastLogTerm := rf.readLog(lastLogIndex)
	// fmt.Printf("服务器 %v 收到 %v 任期的 RPC，自己任期 %v 的RPC\n", rf.me, args.Term, rf.getTerm())

	if args.Term < lastLogTerm {
		return // invaild rpc
	}

	rf.changeState(FOLLOWER)
	rf.updateTerm(args.Term)
	rf.changeElection(false)
	rf.et.reset()

	if args.Entries != nil {
		// check for append and commit
		// fmt.Printf("追随者 %v 收到来自领导者 %v 的上一个索引为 %v 的 append，其自己的 lastIndex 是 %v \n", rf.me, args.LeaderId, args.PrevLogIndex, lastLogIndex)
		if lastLogIndex < args.PrevLogIndex { // 这个追随者前面缺日志
			return
		} else if lastLogIndex > args.PrevLogIndex { // 这个追随者的日志比当前领导者的长，把后面的都删了
			rf.trimRightLog(args.PrevLogIndex + 1)
			lastLogIndex = rf.getLogLen() - 1
			_, lastLogTerm = rf.readLog(lastLogIndex)
		}

		// now lastLogIndex == args.PrevLogIndex
		if lastLogTerm != args.PrevLogTerm { // 日志长度相同，但任期不同
			return
		}

		rf.appendLog(logEntry{args.Entries, args.Term})
		// fmt.Printf("追随者 %v 追加了 %v，当前日志长度 %v，当前任期 %v\n", rf.me, args.Entries, len(rf.log), args.Term)
	}

	rf.commitMu.Lock()
	defer rf.commitMu.Unlock()
	for ; rf.commitIndex < args.LeaderCommit && rf.commitIndex+1 < rf.getLogLen(); rf.commitIndex++ {
		// fmt.Printf("rf.commitIndex + 1 = %v, len(rf.log) = %v\n", rf.commitIndex+1, len(rf.log))
		e, _ := rf.readLog(rf.commitIndex + 1)
		rf.applyCh <- ApplyMsg{true, e, rf.commitIndex + 1}
		// fmt.Printf("追随者 %v 提交并应用了 %v, 当前日志长度 %v，当前 commitIndex %v\n", rf.me, rf.log[rf.commitIndex+1].Entry, len(rf.log), rf.commitIndex+1)
	}

	reply.Success = true
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

// func (rf *Raft) execReplicate(command interface{}, rc *replicateCount) {
// 	for rc.getCount() < len(rf.peers)/2+1 {
// 		time.Sleep(10 * time.Millisecond)
// 		fmt.Printf("领导者 %v，条目 %v 计数 %v\n", rf.me, command, rc.getCount())
// 	}

// 	rf.commitIndex++

// 	rf.applyCh <- ApplyMsg{true, command, rf.commitIndex}
// 	fmt.Printf("领导者 %v 提交并应用了 %v, 当前日志长度 %v, 当前 commitIndex %v\n", rf.me, command, len(rf.log), rf.commitIndex)
// 	n, err := fmt.Println(<-rf.applyCh)
// 	fmt.Println("err: ", n, err)
// 	fmt.Println("test")
// }

func (rf *Raft) startAgreement(index int, command interface{}, rc *replicateCount) {

	args := AppendEntriesArgs{}
	args.Entries = command
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendReplication(i, rc)
	}

	// go rf.execReplicate(command, &rc)
	for rc.getCount() < len(rf.peers)/2+1 {
		time.Sleep(10 * time.Millisecond)
	}

	rf.commitMu.Lock()
	defer rf.commitMu.Unlock()
	for rf.commitIndex < index {
		e, _ := rf.readLog(rf.commitIndex + 1)
		rf.applyCh <- ApplyMsg{true, e, rf.commitIndex + 1}
		rf.commitIndex++
		// fmt.Printf("领导者 %v 提交并应用了 %v, 当前日志长度 %v, 当前 commitIndex %v\n", rf.me, command, len(rf.log), rf.commitIndex)
	}

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	if rf.isState(LEADER) {
		rc := replicateCount{}
		rc.addCount()
		index1 := rf.appendLog(logEntry{command, rf.getTerm()})
		// fmt.Printf("领导者 %v 追加了 %v，当前任期 %v\n", rf.me, command, rf.getTerm())
		go rf.startAgreement(index1, command, &rc)

		index = index1
		term = rf.getTerm()
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
	rf.vi.votedTerm = -1

	// initialize timer info
	rf.et.timeout = rand.Int()%200 + 300 // [300, 500) ms

	rf.log = append(rf.log, logEntry{0, 0}) // initial consistency, the lock is unnecessary

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.timeout()

	return rf
}

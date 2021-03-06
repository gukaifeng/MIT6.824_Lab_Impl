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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
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

type elecState int // state for servers

const (
	FOLLOWER elecState = iota
	CANDADITE
	LEADER
)

const (
	ELECT_TIMEOUT_LEFT = 200 // unit: ms
	ELECT_TIMEOUT_SPAN = 100
	HEARTBEAT_INTERVAL = 50
)

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type Entry struct {
	Command interface{}
	Term    int
}

////////////////////////////////////////////////////////////////

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// current state
	state   elecState
	muState sync.Mutex // Lock to protect shared access to this peer's state

	// current term
	currentTerm   int
	muCurrentTerm sync.Mutex

	// vote grant info
	votedFor    int        // candidateId which this server voted for in current term, -1 means haven't voted yet
	muVotedFor  sync.Mutex // mutex for votedFor
	muGrantVote sync.Mutex // mutex for the whole process of grant a vote

	// vote requested info
	numVotes   int        // the number of votes received
	muNumVotes sync.Mutex // mutex for numVotes
	muBeLeader sync.Mutex // mutex for the process of being a leader

	// log
	log   []Entry
	muLog sync.Mutex // mutex for append and start agreememt in the leader

	muAppendEntries sync.Mutex

	// commit
	commitIndex   int
	muCommitIndex sync.Mutex

	// apply
	lastApplied int
	muApply     sync.Mutex
	applyCh     chan ApplyMsg

	// the leader maintains for the followers
	nextIndex    []int
	matchIndex   []int
	muNextIndex  []sync.Mutex
	muMatchIndex []sync.Mutex
	muAgreement  sync.Mutex

	// election timer
	electTicker   *time.Ticker
	muElectTicker sync.Mutex
	electTimeout  time.Duration

	// heartbeat timer
	heartbeatTicker   *time.Ticker
	muHeartbeatTicker sync.Mutex
	heartbeatInterval time.Duration
}

// state
func (rf *Raft) changeState(s elecState) {
	rf.muState.Lock()
	defer rf.muState.Unlock()
	rf.state = s
}
func (rf *Raft) isState(s elecState) bool {
	rf.muState.Lock()
	defer rf.muState.Unlock()
	return rf.state == s
}

// current term
func (rf *Raft) increaseCurrentTerm() {
	rf.muCurrentTerm.Lock()
	defer rf.muCurrentTerm.Unlock()
	rf.currentTerm++
}
func (rf *Raft) setCurrentTerm(a int) {
	rf.muCurrentTerm.Lock()
	defer rf.muCurrentTerm.Unlock()
	rf.currentTerm = a
}
func (rf *Raft) getCurrentTerm() int {
	rf.muCurrentTerm.Lock()
	defer rf.muCurrentTerm.Unlock()
	return rf.currentTerm
}

// vote grant
func (rf *Raft) setVotedFor(a int) {
	rf.muVotedFor.Lock()
	defer rf.muVotedFor.Unlock()
	rf.votedFor = a
}
func (rf *Raft) getVotedFor() int {
	rf.muVotedFor.Lock()
	defer rf.muVotedFor.Unlock()
	return rf.votedFor
}

// request vote
func (rf *Raft) addVotesNum() {
	rf.muNumVotes.Lock()
	defer rf.muNumVotes.Unlock()
	rf.numVotes++
}
func (rf *Raft) setVotesNum(a int) {
	rf.muNumVotes.Lock()
	defer rf.muNumVotes.Unlock()
	rf.numVotes = a
}
func (rf *Raft) getVotesNum() int {
	rf.muNumVotes.Lock()
	defer rf.muNumVotes.Unlock()
	return rf.numVotes
}

// log
func (rf *Raft) appendLog(e Entry) int {
	rf.muLog.Lock()
	defer rf.muLog.Unlock()
	rf.log = append(rf.log, e)
	return len(rf.log) - 1
}
func (rf *Raft) readLog(i int) Entry {
	rf.muLog.Lock()
	defer rf.muLog.Unlock()
	return rf.log[i]
}
func (rf *Raft) getLogLen() int {
	rf.muLog.Lock()
	defer rf.muLog.Unlock()
	return len(rf.log)
}
func (rf *Raft) trimRightLog(end int) {
	rf.muLog.Lock()
	defer rf.muLog.Unlock()
	rf.log = rf.log[:end]
}
func (rf *Raft) setLog(l []Entry) {
	rf.muLog.Lock()
	defer rf.muLog.Unlock()
	rf.log = l
}
func (rf *Raft) getLog() []Entry {
	rf.muLog.Lock()
	defer rf.muLog.Unlock()
	return rf.log
}

// commit
func (rf *Raft) getCommitIndex() int {
	rf.muCommitIndex.Lock()
	defer rf.muCommitIndex.Unlock()
	return rf.commitIndex
}
func (rf *Raft) setCommitIndex(a int) {
	rf.muCommitIndex.Lock()
	defer rf.muCommitIndex.Unlock()
	rf.commitIndex = a
}

// apply
func (rf *Raft) apply() {
	rf.muApply.Lock()
	defer rf.muApply.Unlock()
	commitIndex := rf.getCommitIndex()
	for ; rf.lastApplied < commitIndex; rf.lastApplied++ {
		c := rf.readLog(rf.lastApplied + 1).Command
		rf.applyCh <- ApplyMsg{true, c, rf.lastApplied + 1}
		// DPrintf("????????? %v???%v ???????????? ??????????????? %v ?????? %v????????????????????? %v????????? commitIndex ??? %v??????????????? %v\n", rf.me, rf.isState(LEADER), rf.lastApplied+1, c, rf.getLogLen(), rf.getCommitIndex(), rf.getCurrentTerm())
	}
}

// the leader maintains
// func (rf *Raft) decreaseNextIndex(i int) {
// 	rf.muNextIndex[i].Lock()
// 	defer rf.muNextIndex[i].Unlock()
// 	rf.nextIndex[i]--
// }
func (rf *Raft) addNextIndex(i, a int) {
	rf.muNextIndex[i].Lock()
	defer rf.muNextIndex[i].Unlock()
	rf.nextIndex[i] += a
}
func (rf *Raft) setNextIndex(i, a int) {
	rf.muNextIndex[i].Lock()
	defer rf.muNextIndex[i].Unlock()
	rf.nextIndex[i] = a
}
func (rf *Raft) getNextIndex(i int) int {
	rf.muNextIndex[i].Lock()
	defer rf.muNextIndex[i].Unlock()
	return rf.nextIndex[i]
}
func (rf *Raft) setMatchIndex(i, a int) {
	rf.muMatchIndex[i].Lock()
	defer rf.muMatchIndex[i].Unlock()
	rf.matchIndex[i] = a
}
func (rf *Raft) getMatchIndex(i int) int {
	rf.muMatchIndex[i].Lock()
	defer rf.muMatchIndex[i].Unlock()
	return rf.matchIndex[i]
}

// election timer
func (rf *Raft) newElectTicker() {
	rf.muElectTicker.Lock()
	defer rf.muElectTicker.Unlock()
	rf.electTicker = time.NewTicker(rf.electTimeout)
}
func (rf *Raft) resetElectTicker() {
	rf.muElectTicker.Lock()
	defer rf.muElectTicker.Unlock()
	rf.electTicker.Reset(rf.electTimeout)
}

// heartbeat timer
func (rf *Raft) newHeartbeatTicker() {
	rf.muHeartbeatTicker.Lock()
	defer rf.muHeartbeatTicker.Unlock()
	rf.heartbeatTicker = time.NewTicker(rf.heartbeatInterval)
}
func (rf *Raft) stopHeartbeatTicker() {
	rf.muHeartbeatTicker.Lock()
	defer rf.muHeartbeatTicker.Unlock()
	rf.heartbeatTicker.Stop()
}
func (rf *Raft) heartbeatTimeout() {
	rf.muHeartbeatTicker.Lock()
	defer rf.muHeartbeatTicker.Unlock()
	<-rf.heartbeatTicker.C
}

////////////////////////////////////////////////////////////////

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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludeTerm   int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.getCurrentTerm())
	e.Encode(rf.getVotedFor())
	e.Encode(rf.getLog())

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		return
	} else {
		rf.setCurrentTerm(currentTerm)
		rf.setVotedFor(votedFor)
		rf.setLog(log)
	}
}

func (rf *Raft) execRequestVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}

	if ok := rf.sendRequestVote(server, args, reply); !ok {
		return
	}

	if rf.getCurrentTerm() != args.Term { // outdated rpc reply
		return
	}

	if reply.Term > rf.getCurrentTerm() {
		rf.changeState(FOLLOWER)
		rf.setCurrentTerm(reply.Term)
		rf.setVotedFor(-1)
		rf.persist()
		return
	}

	if reply.VoteGranted {
		rf.addVotesNum()

		rf.muBeLeader.Lock()
		defer rf.muBeLeader.Unlock()
		if rf.isState(CANDADITE) && rf.getVotesNum() >= len(rf.peers)/2+1 { // received votes from the majority of servers
			// DPrintf("????????? %v ??????????????? %v ????????????\n", rf.me, rf.getCurrentTerm())
			rf.changeState(LEADER)

			nServers := len(rf.peers)
			lenLog := rf.getLogLen()
			for i := 0; i < nServers; i++ {
				rf.setNextIndex(i, lenLog)
				rf.setMatchIndex(i, 0)
			}
			go rf.runHeartbeat()
			go rf.checkMatchIndex()
		}
	}
}

func (rf *Raft) electionTimer() {
	rf.newElectTicker()
	for !rf.killed() {
		<-rf.electTicker.C

		// TODO: I don't understand here
		// 		 and receive AppendEnrties RPC func(),
		//       i.e. heartbeat and append
		// if rf.isState(LEADER) {
		// 	continue
		// }
		rf.resetElectTicker()

		rf.changeState(CANDADITE)
		rf.increaseCurrentTerm()
		rf.persist()

		rvArgs := RequestVoteArgs{}
		rvArgs.CandidateId = rf.me
		rvArgs.Term = rf.getCurrentTerm()
		rvArgs.LastLogIndex = rf.getLogLen() - 1
		rvArgs.LastLogTerm = rf.readLog(rvArgs.LastLogIndex).Term

		rf.setVotesNum(1) // vote for itself
		rf.setVotedFor(rf.me)
		rf.persist()

		// DPrintf("????????? %v ??????????????? %v ?????????????????? %v????????????????????? %v\n", rf.me, rf.getCurrentTerm(), rf.isState(LEADER), len(rf.peers))
		for i := 0; i < len(rf.peers) && rf.isState(CANDADITE); i++ {
			if i != rf.me {
				go rf.execRequestVote(i, &rvArgs)
			}
		}
	}
}

func (rf *Raft) execAppendEntries(server int, index int, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	if !rf.isState(LEADER) || index > rf.getLogLen()-1 {
		return
	}

	args := &AppendEntriesArgs{}
	args.LeaderId = rf.me
	args.Term = rf.getCurrentTerm()
	args.LeaderCommit = rf.getCommitIndex()

	lastIndex := index
	ni := rf.getNextIndex(server)

	for ; lastIndex > ni; lastIndex-- {
		args.Entries = append(args.Entries, rf.readLog(lastIndex))
	}

	for ; rf.isState(LEADER); ni = rf.getNextIndex(server) {
		if ni <= index {
			args.PrevLogIndex = ni - 1
			args.PrevLogTerm = rf.readLog(args.PrevLogIndex).Term

			for ; lastIndex >= ni; lastIndex-- {
				args.Entries = append(args.Entries, rf.readLog(lastIndex))
			}
		} else {
			args.PrevLogIndex = index
			args.PrevLogTerm = rf.readLog(args.PrevLogIndex).Term
		}

		reply := &AppendEntriesReply{}

		if ok := rf.sendAppendEntries(server, args, reply); !ok {
			return
		}

		if rf.getCurrentTerm() != args.Term { // outdated rpc reply
			return
		}

		if !reply.Success {
			if reply.Term > args.Term {
				rf.changeState(FOLLOWER)
				rf.setCurrentTerm(reply.Term)
				rf.setVotedFor(-1)
				rf.persist()
				return
			}
			// conflict response
			found := false
			if reply.ConflictTerm != -1 {
				for i := args.PrevLogIndex; i >= 0; i-- {
					if t := rf.readLog(i).Term; t == reply.ConflictTerm {
						rf.setNextIndex(server, i+1)
						found = true
						break
					}
				}
			}
			if !found {
				rf.setNextIndex(server, reply.ConflictIndex)
			}
		} else {
			if args.Entries != nil && len(args.Entries) > 0 {
				rf.setMatchIndex(server, args.PrevLogIndex+len(args.Entries))
				rf.addNextIndex(server, len(args.Entries))
			}
			rf.resetElectTicker()
			break
		}
	}
}
func (rf *Raft) runHeartbeat() {
	rf.newHeartbeatTicker()
	for rf.isState(LEADER) && !rf.killed() {
		rf.heartbeatTimeout()
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.execAppendEntries(i, rf.getLogLen()-1, nil)
			}
		}
	}
	rf.stopHeartbeatTicker()
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

	if args.Term > rf.getCurrentTerm() {
		rf.changeState(FOLLOWER)
		rf.setCurrentTerm(args.Term)
		rf.setVotedFor(-1)
		rf.persist()
	}

	lastLogIndex := rf.getLogLen() - 1
	lastLogTerm := rf.readLog(lastLogIndex).Term
	if args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		return
	}

	rf.muGrantVote.Lock()
	defer rf.muGrantVote.Unlock()
	if rf.getVotedFor() == -1 {
		rf.resetElectTicker()
		rf.changeState(FOLLOWER)
		rf.setCurrentTerm(args.Term)
		rf.setVotedFor(args.CandidateId)
		rf.persist()
		reply.VoteGranted = true
	}
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.muAppendEntries.Lock()
	defer rf.muAppendEntries.Unlock()

	currTerm := rf.getCurrentTerm()
	reply.Term = currTerm
	if args.Term < currTerm {
		return
	}
	rf.resetElectTicker()
	if args.Term > currTerm {
		rf.setCurrentTerm(args.Term)
		rf.persist()
	}
	if !rf.isState(FOLLOWER) {
		rf.changeState(FOLLOWER)
		rf.setVotedFor(args.LeaderId)
		rf.persist()
	}

	lastLogIndex := rf.getLogLen() - 1
	var lastLogTerm int
	if lastLogIndex < args.PrevLogIndex {
		reply.ConflictIndex = lastLogIndex + 1
		reply.ConflictTerm = -1
		return
	} else {
		lastLogTerm = rf.readLog(lastLogIndex).Term
		t := rf.readLog(args.PrevLogIndex).Term
		if t != args.PrevLogTerm {
			reply.ConflictTerm = t
			for i := args.PrevLogIndex; i >= 0; i-- {
				if rf.readLog(i).Term != t {
					reply.ConflictIndex = i + 1
					break
				}
			}
			return
		}
	}

	if args.Entries != nil {
		nextIndex := len(args.Entries) - 1
		if lastLogIndex > args.PrevLogIndex {
			for j := 1; nextIndex >= 0; nextIndex-- {
				nextLogIndex := args.PrevLogIndex + j
				if nextLogIndex > len(rf.log)-1 {
					break
				}
				nextLogTerm := rf.readLog(nextLogIndex).Term
				if nextLogTerm != args.Entries[nextIndex].Term {
					rf.trimRightLog(nextLogIndex)
					rf.persist()
					break
				}
				j++
			}
		}
		// DPrintf("????????? %v ????????? %v ???????????? %v????????????????????? %v??????????????? %v, ???????????? %v\n",
		// 	rf.me, rf.getLogLen(), args.Entries[0:nextIndex+1], rf.getLogLen(), rf.log, rf.getCurrentTerm())
		for ; nextIndex >= 0; nextIndex-- {
			rf.appendLog(args.Entries[nextIndex])
		}
		rf.persist()
		reply.Success = true
	} else if lastLogIndex == args.PrevLogIndex &&
		lastLogTerm == args.PrevLogTerm {
		reply.Success = true
	}

	if reply.Success {
		minCommitIndex := Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		if minCommitIndex > rf.getCommitIndex() {
			rf.setCommitIndex(minCommitIndex)
		}
		go rf.apply()
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

func (rf *Raft) checkMatchIndex() {
	for rf.isState(LEADER) && !rf.killed() {
		currentTerm := rf.getCurrentTerm()
		lastLogIndex := rf.getLogLen() - 1
		nServer := len(rf.peers)
		majorities := nServer/2 + 1
		for n := rf.getCommitIndex() + 1; n <= lastLogIndex; n++ {
			if rf.readLog(n).Term != currentTerm {
				continue
			}
			count := 1
			for p := 0; p < nServer; p++ {
				if rf.getMatchIndex(p) >= n {
					count++
					if count >= majorities {
						rf.setCommitIndex(n)
						go rf.apply()
						break
					}
				}
			}
			if count < majorities {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) startAgreement(index int, command interface{}) {
	rf.muAgreement.Lock()
	defer rf.muAgreement.Unlock()
	if index <= rf.getCommitIndex() {
		return
	}

	wg := sync.WaitGroup{}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			wg.Add(1)
			go rf.execAppendEntries(i, index, &wg)
		}
	}
	wg.Wait()
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	// Your code here (2B).
	if rf.isState(LEADER) {
		index1 := rf.appendLog(Entry{command, rf.getCurrentTerm()})
		rf.persist()
		// DPrintf("????????? %v ????????? %v ???????????? %v????????????????????? %v??????????????? %v, ???????????? %v\n",
		// 	rf.me, index1, command, rf.getLogLen(), rf.log, rf.getCurrentTerm())
		go rf.startAgreement(index1, command)

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
	rf.votedFor = -1

	// initialize timer info
	rf.electTimeout =
		time.Millisecond *
			time.Duration(rand.Int63()%ELECT_TIMEOUT_SPAN+ELECT_TIMEOUT_LEFT)
	rf.heartbeatInterval =
		time.Millisecond *
			time.Duration(HEARTBEAT_INTERVAL)

	rf.log = append(rf.log, Entry{0, 0}) // initial consistency, the lock is unnecessary
	rf.applyCh = applyCh

	nServers := len(peers)
	rf.nextIndex = make([]int, nServers)
	rf.matchIndex = make([]int, nServers)
	rf.muNextIndex = make([]sync.Mutex, nServers)
	rf.muMatchIndex = make([]sync.Mutex, nServers)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionTimer()

	return rf
}

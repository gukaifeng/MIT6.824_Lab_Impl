package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Master struct {
	files        []string // partitioned files
	timeOut      int
	mapStates    TasksState
	reduceStates TasksState
}

type State int // task state - unprocess, processing or finished

const (
	UNPROCESS State = iota
	PROCESSING
	FINISHED
)

type TasksState struct {
	states  []State
	mutexes []sync.RWMutex
	flags   struct{ ptr, finished, total int }
}

func (t *TasksState) GetFinished() int {
	t.mutexes[t.flags.total+1].RLock()
	defer t.mutexes[t.flags.total+1].RUnlock()
	return t.flags.finished
}

func (t *TasksState) AddFinished() {
	t.mutexes[t.flags.total+1].Lock()
	defer t.mutexes[t.flags.total+1].Unlock()
	t.flags.finished++
}

func (t *TasksState) GetState(n int) State {
	t.mutexes[n].RLock()
	defer t.mutexes[n].RUnlock()
	return t.states[n]
}

// change state s1 -> s2
// if force is true, t.states[n] will be changed to s2 even if the original value was not s1
// if force is false and the original value of t.states[n] is not s1, no changes will be made
// return: whether the change have been applied
func (t *TasksState) ChangeState(n int, s1 State, s2 State, force bool) bool {
	t.mutexes[n].Lock()
	defer t.mutexes[n].Unlock()
	if t.states[n] == s1 || force {
		t.states[n] = s2
		return true
	}
	return false
}

func (t *TasksState) GetAndAddPtr() int {
	t.mutexes[t.flags.total].Lock()
	defer t.mutexes[t.flags.total].Unlock()
	p := t.flags.ptr
	t.flags.ptr = (t.flags.ptr + 1) % t.flags.total
	return p
}

// RPC handlers for the worker to call.

func (m *Master) Assgin(args *Args, reply *Reply) error {
	if m.reduceDone() {
		reply.Finished = true
		return nil
	}

	if !m.mapDone() {
		m.assginMapTask(reply)
	} else {
		m.assginReduceTask(reply)
	}

	if reply.IsAssgined {
		reply.M = m.mapStates.flags.total
		reply.R = m.reduceStates.flags.total
	}

	return nil
}

func (m *Master) assginMapTask(reply *Reply) {
	ms := &m.mapStates
	for i := 0; i < ms.flags.total; i++ {
		p := ms.GetAndAddPtr()
		if ms.ChangeState(p, UNPROCESS, PROCESSING, false) {
			reply.IsAssgined = true
			reply.MapT.SeqNum = p
			reply.MapT.PartitionedFile = m.files[p]
			go m.timeout(p, ms)
			break
		}
	}
}

func (m *Master) assginReduceTask(reply *Reply) {
	rs := &m.reduceStates
	for i := 0; i < rs.flags.total; i++ {
		p := rs.GetAndAddPtr()
		if rs.ChangeState(p, UNPROCESS, PROCESSING, false) {
			reply.IsAssgined = true
			reply.ReduceT.SeqNum = p
			go m.timeout(p, rs)
			break
		}
	}
}

func (m *Master) timeout(n int, t *TasksState) {
	for i := 0; i < m.timeOut; i++ {
		time.Sleep(time.Second)
		if t.GetState(n) == FINISHED {
			return
		}
	}
	t.ChangeState(n, PROCESSING, UNPROCESS, true)
}

func (m *Master) FinishMap(args *Args, reply *Reply) error {
	m.mapStates.ChangeState(args.SeqNum, PROCESSING, FINISHED, true)
	m.mapStates.AddFinished()
	return nil
}

func (m *Master) FinishReduce(args *Args, reply *Reply) error {
	m.reduceStates.ChangeState(args.SeqNum, PROCESSING, FINISHED, true)
	m.reduceStates.AddFinished()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	if m.reduceDone() {
		// delete all intermediate data
		for i := 0; i < m.mapStates.flags.total; i++ {
			for j := 0; j < m.reduceStates.flags.total; j++ {
				file := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
				os.Remove(file)
			}
		}
		return true
	}
	return false
}

func (m *Master) mapDone() bool {
	return m.mapStates.GetFinished() == m.mapStates.flags.total
}

func (m *Master) reduceDone() bool {
	return m.reduceStates.GetFinished() == m.reduceStates.flags.total
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{files: files}

	m.mapStates.states = make([]State, len(files))
	m.reduceStates.states = make([]State, nReduce)

	m.mapStates.mutexes = make([]sync.RWMutex, len(files)+2) // the last 2 mutexes are for the ptr and finished variable
	m.reduceStates.mutexes = make([]sync.RWMutex, nReduce+2)

	m.mapStates.flags.total = len(files)
	m.reduceStates.flags.total = nReduce

	m.timeOut = 10 // unit: second

	m.server()
	return &m
}

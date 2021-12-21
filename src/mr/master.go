package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type Master struct {
	files []string // partitioned files

	mapStates    TasksState
	reduceStates TasksState

	timeOut int // timeout
}

type State int // task state - unprocess, processing or finished

const (
	UNPROCESS State = iota
	PROCESSING
	FINISHED
)

type TasksState struct {
	states   []State
	total    int
	ptr      int
	finished int
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
		reply.NMap = m.mapStates.total
		reply.NReduce = m.reduceStates.total
	}

	return nil
}

func (m *Master) FinishMap(args *Args, reply *Reply) error {
	m.mapStates.states[args.SeqNum] = FINISHED
	m.mapStates.finished++
	return nil
}

func (m *Master) FinishReduce(args *Args, reply *Reply) error {
	m.reduceStates.states[args.SeqNum] = FINISHED
	m.reduceStates.finished++
	return nil
}

func (m *Master) assginMapTask(reply *Reply) {
	ms := &m.mapStates
	for i := 0; i < ms.total; i++ {
		if ms.states[ms.ptr] == UNPROCESS {
			reply.IsAssgined = true
			reply.MapT.SeqNum = ms.ptr
			reply.MapT.PartitionedFile = m.files[ms.ptr]
			ms.states[ms.ptr] = PROCESSING
			go m.timeout(ms.ptr, ms.states)
			break
		}
		ms.ptr = (ms.ptr + 1) % ms.total
	}
}

func (m *Master) assginReduceTask(reply *Reply) {
	rs := &m.reduceStates
	for i := 0; i < rs.total; i++ {
		if rs.states[rs.ptr] == UNPROCESS {
			reply.IsAssgined = true
			reply.ReduceT.SeqNum = rs.ptr
			rs.states[rs.ptr] = PROCESSING
			go m.timeout(rs.ptr, rs.states)
			break
		}
		rs.ptr = (rs.ptr + 1) % rs.total
	}
}

func (m *Master) timeout(seqnum int, states []State) {
	for i := 0; i < m.timeOut; i++ {
		time.Sleep(time.Second)
		if states[seqnum] == FINISHED {
			return
		}
	}
	states[seqnum] = UNPROCESS
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
		for i := 0; i < m.mapStates.total; i++ {
			for j := 0; j < m.reduceStates.total; j++ {
				file := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
				os.Remove(file)
			}
		}
		return true
	}
	return false
}

func (m *Master) mapDone() bool {
	return m.mapStates.finished == m.mapStates.total
}

func (m *Master) reduceDone() bool {
	return m.reduceStates.finished == m.reduceStates.total
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

	m.mapStates.total = len(files)
	m.reduceStates.total = nReduce

	m.timeOut = 10 // unit: second

	m.server()
	return &m
}

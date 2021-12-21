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

	mapStates    []State // map tasks status
	reduceStates []State // reduce tasks status

	timeOut int // timeout
}

type State int // task state - unprocess, processing or finished

const (
	UNPROCESS State = iota
	PROCESSING
	FINISHED
)

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

	reply.NMap = len(m.mapStates)
	reply.NReduce = len(m.reduceStates)

	return nil
}

func (m *Master) FinishMap(args *Args, reply *Reply) error {
	m.mapStates[args.SeqNum] = FINISHED
	return nil
}

func (m *Master) FinishReduce(args *Args, reply *Reply) error {
	m.reduceStates[args.SeqNum] = FINISHED
	return nil
}

func (m *Master) assginMapTask(reply *Reply) {
	for i := 0; i < len(m.mapStates); i++ {
		if m.mapStates[i] == UNPROCESS {
			reply.IsAssgined = true
			reply.MapT.SeqNum = i
			reply.MapT.PartitionedFile = m.files[i]
			m.mapStates[i] = PROCESSING
			go m.timeout(i, m.mapStates)
			break
		}
	}
}

func (m *Master) assginReduceTask(reply *Reply) {
	for i := 0; i < len(m.reduceStates); i++ {
		if m.reduceStates[i] == UNPROCESS {
			reply.IsAssgined = true
			reply.ReduceT.SeqNum = i
			m.reduceStates[i] = PROCESSING
			go m.timeout(i, m.reduceStates)
			break
		}
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
		for i := 0; i < len(m.mapStates); i++ {
			for j := 0; j < len(m.reduceStates); j++ {
				file := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
				os.Remove(file)
			}
		}
		return true
	}
	return false
}

func (m *Master) mapDone() bool {
	for i := 0; i < len(m.mapStates); i++ {
		if m.mapStates[i] != FINISHED {
			return false
		}
	}
	return true
}

func (m *Master) reduceDone() bool {
	for i := 0; i < len(m.reduceStates); i++ {
		if m.reduceStates[i] != FINISHED {
			return false
		}
	}
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{files: files}

	m.mapStates = make([]State, len(files))
	m.reduceStates = make([]State, nReduce)

	m.timeOut = 10 // unit: second

	m.server()
	return &m
}

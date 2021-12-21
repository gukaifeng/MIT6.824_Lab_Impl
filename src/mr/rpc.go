package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type MapTask struct {
	SeqNum          int
	PartitionedFile string
}

type ReduceTask struct {
	SeqNum int
}
type Args struct {
	SeqNum int
}

type Reply struct {
	NReduce    int // nReduce
	NMap       int // nMap
	MapT       MapTask
	ReduceT    ReduceTask
	IsAssgined bool
	Finished   bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

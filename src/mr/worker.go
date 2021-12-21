package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := Args{}
		reply := Reply{}

		CallMaster("Master.Assgin", &args, &reply)

		if reply.Finished {
			break
		}

		if !reply.IsAssgined {
			time.Sleep(time.Second)
			continue
		}

		// if reply.MapT.PartitionedFile != "", this is a map task,
		// otherwise a reduce task
		if reply.MapT.PartitionedFile != "" {
			ExecMapFunc(reply, mapf)
		} else {
			ExecReduceFunc(reply, reducef)
		}
	}
}

func ExecMapFunc(reply Reply, mapf func(string, string) []KeyValue) {
	seqnum := reply.MapT.SeqNum
	partitionedfile := reply.MapT.PartitionedFile

	file, err := os.Open(partitionedfile)
	if err != nil {
		log.Fatalf("cannot open %v", partitionedfile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", partitionedfile)
	}

	defer file.Close()

	kva := mapf(partitionedfile, string(content))
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		X := ihash(kv.Key) % reply.NReduce
		intermediate[X] = append(intermediate[X], kv)
	}

	for i := 0; i < reply.NReduce; i++ {
		// write intermediate file mr-X-Y, X is rap task num, Y is reduce task num
		oname := "mr-" + strconv.Itoa(seqnum) + "-" + strconv.Itoa(i)
		tmpfile, _ := ioutil.TempFile(".", oname+"-tmp-*")
		enc := json.NewEncoder(tmpfile)
		for _, kv := range intermediate[i] {
			enc.Encode(&kv)
		}

		os.Rename(tmpfile.Name(), oname)
		defer tmpfile.Close()
	}
	CallMaster("Master.FinishMap", &Args{SeqNum: seqnum}, nil)
}

func ExecReduceFunc(reply Reply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	seqnum := reply.ReduceT.SeqNum

	for i := 0; i < reply.NMap; i++ {
		iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(seqnum)
		ifile, _ := os.Open(iname)
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		defer ifile.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(seqnum)
	tmpfile, _ := ioutil.TempFile(".", oname+"-tmp-*")

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	os.Rename(tmpfile.Name(), oname)
	defer tmpfile.Close()

	CallMaster("Master.FinishReduce", &Args{SeqNum: seqnum}, nil)
}

func CallMaster(rpcname string, args interface{}, reply interface{}) {
	call(rpcname, args, reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

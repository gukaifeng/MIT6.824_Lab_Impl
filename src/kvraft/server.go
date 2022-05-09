package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Operation string
	ClientId  int
	Seq       int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db              map[string]string
	lastIndex       int
	clientLastApply map[int]int
}

func (kv *KVServer) get(key string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.db[key]; ok {
		return v, true
	}
	return "", false
}

func (kv *KVServer) apply() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.CommandValid {
			c := msg.Command.(Op)

			if c.Seq <= kv.clientLastApply[c.ClientId] {
				continue
			}

			kv.mu.Lock()
			if c.Operation == "Put" {
				kv.db[c.Key] = c.Value
			} else {
				kv.db[c.Key] += c.Value
			}
			kv.lastIndex = msg.CommandIndex
			kv.mu.Unlock()

			kv.clientLastApply[c.ClientId] = c.Seq

			DPrintf("kvraft %v, apply 成功 %v", kv.me, c)
		} else if msg.SnapshotValid {
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				r := bytes.NewBuffer(msg.Snapshot)
				d := labgob.NewDecoder(r)

				var db map[string]string
				if d.Decode(&db) != nil {
					return
				} else {
					kv.db = db
				}
			}
		}

		time.Sleep(20 * time.Millisecond)
	}
}

func (kv *KVServer) makeSnapshot() {
	for !kv.killed() {
		if kv.rf.GetRaftStateSize() > int(float64(kv.maxraftstate)*0.8) {
			kv.mu.Lock()
			w := new(bytes.Buffer)
			s := labgob.NewEncoder(w)
			s.Encode(kv.db)
			data := w.Bytes()
			go kv.rf.Snapshot(kv.lastIndex, data)
			kv.mu.Unlock()
			time.Sleep(250 * time.Millisecond)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, b := kv.rf.GetState(); !b {
		reply.Err = ErrWrongLeader
		return
	}

	entry := Op{args.Key, "", "", args.ClientId, args.Seq}
	index, _, leader := kv.rf.Start(entry)
	DPrintf("Get: %v", entry)
	if !leader {
		reply.Err = ErrWrongLeader
		return
	}
	for kv.rf.GetLastApplied() < index {
		DPrintf("Get 等待中，内容是 %v", entry)
		time.Sleep(10 * time.Millisecond)
		continue
	}
	// cmd := kv.rf.ReadLog(index).Command.(Op)
	// if cmd.Seq == args.Seq {
	// 	reply.Err = OK
	// }

	v, exist := kv.get(args.Key)
	if !exist {
		reply.Err = ErrNoKey
		return
	}

	reply.Err = OK
	reply.Value = v

	DPrintf("Get: %v", args.Key)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, b := kv.rf.GetState(); !b {
		reply.Err = ErrWrongLeader
		return
	}

	entry := Op{args.Key, args.Value, args.Op, args.ClientId, args.Seq}
	index, _, leader := kv.rf.Start(entry)
	DPrintf("PutAppend: %v", entry)
	if !leader {
		reply.Err = ErrWrongLeader
		return
	}
	for kv.rf.GetLastApplied() < index {
		DPrintf("Append 等待中，内容是 %v", entry)
		time.Sleep(10 * time.Millisecond)
		continue
	}
	cmd := kv.rf.ReadLog(index).Command.(Op)
	if cmd.Seq == args.Seq {
		reply.Err = OK
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.clientLastApply = make(map[int]int)

	go kv.apply()
	// go kv.makeSnapshot()

	return kv
}

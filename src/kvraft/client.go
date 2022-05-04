package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"../labrpc"
)

var kvClientId int

func produceClientId() int {
	kvClientId++
	return kvClientId
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	prevLeader     int
	clientId       int
	nextRequestSeq int
	muOperate      sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.prevLeader = -1
	ck.clientId = produceClientId()
	ck.nextRequestSeq = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	// ck.muOperate.Lock()
	// defer ck.muOperate.Unlock()

	args := &GetArgs{key, ck.clientId, ck.nextRequestSeq}

	for {
		reply := &GetReply{}

		if ck.prevLeader == -1 {
			ck.prevLeader = int(nrand()) % len(ck.servers)
		}

		// ok := ck.servers[ck.prevLeader].Call("KVServer.Get", args, reply)
		var ok bool
		t0 := time.Now()
		go ck.sendGet(ck.prevLeader, args, reply, &ok)

		for time.Since(t0).Milliseconds() < 100 {
			if ok {
				break
			}
		}

		if !ok || reply.Err == ErrWrongLeader {
			ck.prevLeader = -1
		} else if reply.Err == OK {
			ck.nextRequestSeq++
			return reply.Value
		} else {
			break
		}
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// You will have to modify this function.
	// ck.muOperate.Lock()
	// defer ck.muOperate.Unlock()

	args := &PutAppendArgs{key, value, op, ck.clientId, ck.nextRequestSeq}

	for {
		DPrintf("客户端尝试 k = %v, v = %v, op = %v", key, value, op)
		reply := &PutAppendReply{}
		if ck.prevLeader == -1 {
			ck.prevLeader = int(nrand()) % len(ck.servers)
		}

		// ok := ck.servers[ck.prevLeader].Call("KVServer.PutAppend", args, reply)
		var ok bool
		t0 := time.Now()
		go ck.sendPutAppend(ck.prevLeader, args, reply, &ok)

		for time.Since(t0).Milliseconds() < 100 {
			if ok {
				break
			}
		}

		if !ok || reply.Err == ErrWrongLeader {
			ck.prevLeader = -1
		} else if reply.Err == OK {
			ck.nextRequestSeq++
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply, ok *bool) {
	*ok = ck.servers[server].Call("KVServer.Get", args, reply)
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply, ok *bool) {
	*ok = ck.servers[server].Call("KVServer.PutAppend", args, reply)
}

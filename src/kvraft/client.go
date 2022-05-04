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

type RPCState struct {
	state   bool
	muState sync.Mutex
}

func (s *RPCState) setRPCState(ok bool) {
	s.muState.Lock()
	defer s.muState.Unlock()
	s.state = ok
}
func (s *RPCState) getRPCState() bool {
	s.muState.Lock()
	defer s.muState.Unlock()
	return s.state
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
	ck.prevLeader = int(nrand()) % len(ck.servers)
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

	ck.muOperate.Lock()
	defer ck.muOperate.Unlock()

	args := &GetArgs{key, ck.clientId, ck.nextRequestSeq}

	for {
		reply := &GetReply{}

		if ck.prevLeader == -1 {
			ck.prevLeader = int(nrand()) % len(ck.servers)
		}

		// ok := ck.servers[ck.prevLeader].Call("KVServer.Get", args, reply)
		// var ok bool
		s := &RPCState{}
		t0 := time.Now()
		go ck.sendGet(ck.prevLeader, args, reply, s)

		for time.Since(t0).Milliseconds() < 280 {
			if s.getRPCState() {
				break
			}
		}

		if !s.getRPCState() || reply.Err == ErrWrongLeader {
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

	ck.muOperate.Lock()
	defer ck.muOperate.Unlock()

	args := &PutAppendArgs{key, value, op, ck.clientId, ck.nextRequestSeq}

	for i := 1; true; i++ {
		DPrintf("客户端 %v 开始第 %v 次尝试 k = %v, v = %v, op = %v，访问的服务器是 %v",
			ck.clientId, i, key, value, args, ck.prevLeader)
		reply := &PutAppendReply{}
		s := &RPCState{}
		t0 := time.Now()
		go ck.sendPutAppend(ck.prevLeader, args, reply, s)

		for time.Since(t0).Milliseconds() < 280 {
			if s.getRPCState() {
				break
			}
		}

		if !s.getRPCState() {
			DPrintf("客户端 %v 第 %v 次尝试 k = %v, v = %v, op = %v 失败，访问的服务器是 %v, 原因是 RPC 超时未返回",
				ck.clientId, i, key, value, args, ck.prevLeader)
			ck.prevLeader = (ck.prevLeader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == ErrWrongLeader {
			DPrintf("客户端 %v 第 %v 次尝试 k = %v, v = %v, op = %v 失败，访问的服务器是 %v, 原因是访问的服务器不是领导者",
				ck.clientId, i, key, value, args, ck.prevLeader)
			ck.prevLeader = (ck.prevLeader + 1) % len(ck.servers)
		} else if reply.Err == OK {
			DPrintf("客户端 %v 第 %v 次尝试 k = %v, v = %v, op = %v 成功，访问的服务器是 %v",
				ck.clientId, i, key, value, args, ck.prevLeader)
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

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply, s *RPCState) {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	s.setRPCState(ok)

}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply, s *RPCState) {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	s.setRPCState(ok)
}

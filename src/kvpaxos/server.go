package kvpaxos

import (
	"cse-513/src/paxos"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
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

	Instance  int //Paxos instance
	Key       string
	Value     string
	Operation string /* We have 3 operations: PUT, GET, APPEND*/
	Seq       int
	RequestID int
	Error     Err
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos
	// Your definitions here.
	requestID2Seq   sync.Map
	dbKey2Value     sync.Map
	lastExecutedSeq int
}

/*
Hint: your code will need to wait for Paxos instances to complete agreement.
The only way to do this is to periodically call Status(), sleeping between
calls.
*/
func (kv *KVPaxos) wait(seq int) bool {
	to := 10 * time.Millisecond
	for i := 0; i < 10; i++ {
		status, _ := kv.px.Status(seq)
		if status == paxos.Decided {
			return true
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
	return false
}

/*
Store/Update the data structure (dbKey2Value) holding key-value pair based on the
PUT or APPEND operation.
*/
func (kv *KVPaxos) UpdateDBKey2Value(op Op) {
	var prevStrValue string
	//Perform "Put" update
	if op.Operation == "Put" {
		kv.dbKey2Value.Store(op.Key, op.Value)
		//Perform "Append" update
	} else if op.Operation == "Append" {
		prevValue, ok := kv.dbKey2Value.Load(op.Key)
		if !ok {
			//if no key present in the db (dbKey2Value)
			prevStrValue = ""
		} else {
			prevStrValue = prevValue.(string)
		}
		kv.dbKey2Value.Store(op.Key, prevStrValue+op.Value)
	}
}

/*
Update the data structure handling the Requested ID and the sequence that was executed last
*/
func (kv *KVPaxos) UpdateRequestID2Seq(op Op) {
	// perform the operation on database before updating the requestID2Seq map
	if op.Operation != "Get" {
		kv.UpdateDBKey2Value(op)
	}
	//update the requestID2Seq map
	seq, ok := kv.requestID2Seq.Load(op.RequestID)
	if ok {
		if op.Seq > seq.(int) {
			kv.requestID2Seq.Store(op.RequestID, op.Seq)
		} else {
			log.Printf("\nExpected sequence number fro client to be integer. Integer Type cast error.")
		}
	} else {
		kv.requestID2Seq.Store(op.RequestID, op.Seq)
	}
}

/*
check if the response from paxos is for the request sent for that request
via that particular kvpaxos sever
*/
func CompareRequest(op1 *Op, op2 *Op) bool {
	return op1.Key == op2.Key &&
		op1.Value == op2.Value &&
		op1.Operation == op2.Operation &&
		op1.RequestID == op2.RequestID &&
		op1.Seq == op2.Seq &&
		op1.Error == op2.Error
}

func (kv *KVPaxos) StartPaxos(op Op) {
	var paxosResp bool
	//run untill we get any response for the request or the request is timed-out
	for {
		kv.lastExecutedSeq += 1
		//start paxos
		kv.px.Start(kv.lastExecutedSeq, op)
		//wait until the request gets any response - checking the status
		paxosResp = kv.wait(kv.lastExecutedSeq)
		if paxosResp {
			status, tempOp := kv.px.Status(kv.lastExecutedSeq)
			if status == paxos.Decided {
				decidedOp, ok := tempOp.(Op)
				if ok {
					kv.UpdateRequestID2Seq(decidedOp)
					kv.px.Done(kv.lastExecutedSeq)
					if CompareRequest(&decidedOp, &op) {
						break
					}
				} else {
					log.Printf("\nType casting of value returned by px.Status failed")
				}
			}
		} else {
			log.Printf("\nRequest Timeout!!")
		}

	}
}

/*
1. Interpret the log before that point to make sure its key/value
database refelects all recents Put()s
2. a kvpaxos server should not complete a Get() RPC if
it is not part of a majority (so that it does not serve stale data).
i.e each Get() (as well as each Put() and Append()) must involve Paxos agreement.
*/
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	seq, ok := kv.requestID2Seq.Load(args.RequestID)
	if ok {
		if seq.(int) >= args.Seq {
			value, err := kv.dbKey2Value.Load(args.Key)
			if !err {
				reply.Err = ErrMissingKey
				return nil
			}
			reply.Err = OK
			reply.Value = value.(string)
			return nil
		}
	}

	op := Op{Operation: "Get",
		Key:       args.Key,
		Seq:       args.Seq,
		Instance:  kv.me,
		RequestID: args.RequestID,
	}
	kv.StartPaxos(op)
	val, ok := kv.dbKey2Value.Load(args.Key)
	if !ok {
		reply.Err = ErrNoKey
		return nil
	}
	reply.Err = OK
	reply.Value = val.(string)
	return nil
}

/* Should enter a PUT or APPEND OUTPUT in the Paxos log,*/
func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	seq, ok := kv.requestID2Seq.Load(args.RequestID)
	if ok {
		if seq.(int) >= args.Seq {
			reply.Err = OK
			return nil // no error
		}
	}

	op := Op{Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		Seq:       args.Seq,
		RequestID: args.RequestID,
		Instance:  kv.me,
	}
	kv.StartPaxos(op)
	reply.Err = OK
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.dbKey2Value = sync.Map{}
	kv.requestID2Seq = sync.Map{}

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}

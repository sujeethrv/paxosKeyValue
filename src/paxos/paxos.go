package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

// Acceptor state for this instance
// acceptor's state:
// n_p (highest prepare seen)
// n_a, v_a (highest accept seen)
type AcceptorState struct {
	n_p int // highest prepare:: prop_num - just integer
	n_a int // highest accept:: prop_num - just integer
	v_a interface{}
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.

	// Map to maintain consensus status for each seq
	seq2Status sync.Map
	// Map to maintain proposal number status for each seq
	seq2PropNum sync.Map
	// Map seq to consensus value.
	seq2ConsVal sync.Map
	// Map seq to acceptor state
	seq2AcceptorState sync.Map
	// Map to track highest Done sequence for each peer
	peer2HighestDoneSeq sync.Map
}

type PrepareArgs struct {
	Seq     int
	PropNum int
}

type PrepareReply struct {
	OK            bool
	N_A           int
	HighestDone   int //piggybacking to implement Min()
	AcceptedValue interface{}
}

type AcceptArgs struct {
	Seq     int
	PropNum int
	Value   interface{}
}

type DecidedArgs struct {
	Seq     int
	PropNum int
	Value   interface{}
}

type AcceptReply struct {
	OK bool
	//PropNum int
}

type DecidedReply struct {
	OK bool
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {

	go func(seq int, v interface{}) {
		// sanity check.
		if seq < px.Min() {
			return
		}
		_, exists := px.seq2Status.Load(seq)

		if !exists {
			// Sequence not present, so run Paxos
			px.seq2Status.Store(seq, Pending)
			px.seq2PropNum.Store(seq, px.me+1) //lowest proposal number is 0. proposal number as -1 is being used indicate as consensus reached in acceptoraccept response
			px.Proposer(seq, v)
		}
	}(seq, v)
}

// Updates the status and does not return anything
func (px *Paxos) Proposer(seq int, value interface{}) {
	var success bool
	var acceptedValue interface{}
	var acceptSuccess bool

	for !px.isdead() {
		status, ok := px.seq2Status.Load(seq)
		if ok && status == Decided {
			break
		}

		propNumInterface, ok := px.seq2PropNum.Load(seq)
		if !ok {
			// Handle case when propNum == nil, hence initializing it
			propNumInterface = px.me + 1
		}

		propNum, ok := propNumInterface.(int)
		if !ok {
			// Handles non int value: by initializaing propNum (non zero value)
			log.Printf("expected propNum to be an int, got %T", propNumInterface)
			propNum = px.me + 1
		}
		px.seq2PropNum.Store(seq, propNum+len(px.peers))
		success, acceptedValue = px.ProposerPrepare(propNum, seq)

		if success {
			//no proposal was accepted
			if acceptedValue == nil {
				acceptedValue = value
			}
			px.ForgetDone(px.Min())
			acceptSuccess = px.ProposerAccept(propNum, acceptedValue, seq)
			if acceptSuccess {
				px.seq2Status.Store(seq, Decided)
				px.seq2ConsVal.Store(seq, acceptedValue)
				px.ProposerDecided(propNum, acceptedValue, seq)
				return
			} else {
				sleepDuration := time.Duration(100+rand.Intn(900)) * time.Millisecond
				time.Sleep(sleepDuration)
			}
		} else {
			// Handles livelock: Sleep for a random duration between 5 to 10 seconds before retrying
			sleepDuration := time.Duration(100+rand.Intn(900)) * time.Millisecond
			time.Sleep(sleepDuration)
		}
	}
}

// runs prepare for a given seq. sends prepare req to all including self.
// if majority respond with promise
// then return (True and v_a of min) else False,NULL
//
// Hint: in order to pass tests assuming unreliable network, your paxos should call the local acceptor
// through a function call rather than RPC.
func (px *Paxos) ProposerPrepare(propNum int, seq int) (bool, interface{}) {
	var count = 0
	var highestNA int = 0
	var acceptedValue interface{} = nil

	// Wait for all goroutines to finish
	var wg sync.WaitGroup

	for i, peer := range px.peers {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int, peer string, count *int, highestNA *int, acceptedValue *interface{}) {
			defer wg.Done()
			args := &PrepareArgs{
				Seq:     seq,
				PropNum: propNum,
			}
			var reply PrepareReply

			// Check if the current peer is local (based on the hint)
			if i == px.me {
				// Directly call the local function is node is to send call to itself
				px.AcceptorPrepare(args, &reply)
				//checking if any acceptor returned the consensus
				if reply.OK {
					px.mu.Lock()
					*count += 1
					if (reply.N_A > *highestNA) && (reply.N_A != propNum) {
						*highestNA = reply.N_A
						*acceptedValue = reply.AcceptedValue
					}
					px.UpdatePeer2HighestDoneSeq(reply.HighestDone, i)
					px.mu.Unlock()
				}
			} else {
				//sending AcceptorPerpare call to other nodes
				var success bool = false
				success = call(peer, "Paxos.AcceptorPrepare", args, &reply)
				if success && reply.OK {
					px.mu.Lock()
					*count++
					if (reply.N_A > *highestNA) && (reply.N_A != propNum) {
						*highestNA = reply.N_A
						*acceptedValue = reply.AcceptedValue
					}
					px.UpdatePeer2HighestDoneSeq(reply.HighestDone, i)
					px.mu.Unlock()
				} else {
					log.Printf("Prepare request timeout. No response from peer: %s \n", peer)
				}
			}
		}(&wg, i, peer, &count, &highestNA, &acceptedValue)
	}

	wg.Wait()
	if count > len(px.peers)/2 {
		if highestNA == 0 {
			return true, nil
		}
		return true, acceptedValue
	}
	return false, nil
}

// sends accept request for a given seq for all servers including self.
// if majority accept, returns True(need not return consensus value right?)
// else returns False
func (px *Paxos) ProposerAccept(propNum int, value interface{}, seq int) bool {
	var count = 0

	// Wait for all goroutines to finish
	var wg sync.WaitGroup

	for i, peer := range px.peers {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int, peer string, count *int) {
			defer wg.Done()
			args := &AcceptArgs{
				Seq:     seq,
				PropNum: propNum,
				Value:   value,
			}
			var reply AcceptReply

			// Check if the current peer is local (based on the hint)
			if i == px.me {
				// Directly call the local function
				px.AcceptorAccept(args, &reply)
				// Check the reply and update count
				if reply.OK {
					px.mu.Lock()
					*count++
					px.mu.Unlock()
				}
			} else {
				//sending AcceptorAccept call to other nodes
				var success bool = false
				success = call(peer, "Paxos.AcceptorAccept", args, &reply)
				if success && reply.OK {
					px.mu.Lock()
					*count++
					px.mu.Unlock()
				} else {
					log.Printf("Accept request timeout. No response from peer: %s \n", peer)
				}
			}
		}(&wg, i, peer, &count)
	}

	wg.Wait()
	return count > len(px.peers)/2
}

// Proposer Decide
func (px *Paxos) ProposerDecided(propNum int, value interface{}, seq int) bool {
	var count = 0

	// A wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	for i, peer := range px.peers {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int, peer string, count *int) {
			defer wg.Done()
			args := &DecidedArgs{
				Seq:     seq,
				PropNum: propNum,
				Value:   value,
			}
			var reply DecidedReply

			// Check if the current peer is local
			if i == px.me {
				// Directly call the local function
				px.AcceptorDecided(args, &reply)
				// Check the reply and update count
				if reply.OK {
					px.mu.Lock()
					*count++
					px.mu.Unlock()
				}
			} else {
				var success bool = false
				success = call(peer, "Paxos.AcceptorDecided", args, &reply)
				if success && reply.OK {
					px.mu.Lock()
					*count++
					px.mu.Unlock()
				} else {
					log.Printf("Accept request timeout. No response from peer: %s \n", peer)
				}
			}
		}(&wg, i, peer, &count)
	}

	wg.Wait()
	return count > len(px.peers) // OR RETURN highest proposal number seen?
}

func (px *Paxos) AcceptorDecided(args *DecidedArgs, reply *DecidedReply) error {
	px.seq2Status.Store(args.Seq, Decided)
	px.seq2ConsVal.Store(args.Seq, args.Value)
	acptState, exists := px.seq2AcceptorState.Load(args.Seq)
	// Initialize the state for this seq if it doesn't exist yet
	if !exists {
		acptState = &AcceptorState{
			n_p: args.PropNum,
			n_a: args.PropNum,
			v_a: args.Value,
		}
		px.seq2AcceptorState.Store(args.Seq, acptState)
	} else {
		acptState.(*AcceptorState).n_a = args.PropNum
		acptState.(*AcceptorState).n_p = args.PropNum
		acptState.(*AcceptorState).v_a = args.Value
	}
	px.seq2PropNum.Store(args.Seq, args.PropNum)
	return nil
}

// Note: checks if consensus has been reached for given "seq",
// if consensus is reached then it rejects prepare req,
// responds/informs consensus has reached and also sends consensus value.
func (px *Paxos) AcceptorPrepare(args *PrepareArgs, reply *PrepareReply) error {

	// Retrieve the current acceptor state for given seq
	acptState, exists := px.seq2AcceptorState.Load(args.Seq)

	// Initialize the state for given seq (if it doesn't exist yet)
	if !exists {
		acptState = &AcceptorState{
			n_p: 0,
			n_a: 0,
			v_a: nil,
		}
		px.seq2AcceptorState.Store(args.Seq, acptState)
	}

	propNumber := args.PropNum

	// If the proposal number is higher than any seen before, accept the prepare request
	if propNumber > acptState.(*AcceptorState).n_p {
		acptState.(*AcceptorState).n_p = propNumber
		reply.OK = true
		reply.N_A = acptState.(*AcceptorState).n_a
		reply.AcceptedValue = acptState.(*AcceptorState).v_a
		highestDone, _ := px.peer2HighestDoneSeq.Load(px.me)
		reply.HighestDone = highestDone.(int)
	} else {
		// Reject the prepare request
		reply.OK = false
	}
	return nil
}

// Note: checks if consensus has been reached for given "seq",
// if consensus is reached then it rejects Accept req,
// informs consensus has reached and also sends consensus value
// Include general accept_req logic
func (px *Paxos) AcceptorAccept(args *AcceptArgs, reply *AcceptReply) error {

	// Retrieve the current acceptor state for this seq
	acptState, exists := px.seq2AcceptorState.Load(args.Seq)

	// Initialize the state for this seq if it doesn't exist yet
	if !exists {
		acptState = &AcceptorState{
			n_p: 0,
			n_a: 0,
			v_a: nil,
		}
		px.seq2AcceptorState.Store(args.Seq, acptState)
	}

	propNumber := args.PropNum

	// If the proposal number is higher than or equal to any seen before, accept the proposal
	if propNumber >= acptState.(*AcceptorState).n_p {
		acptState.(*AcceptorState).n_p = propNumber
		acptState.(*AcceptorState).n_a = propNumber
		acptState.(*AcceptorState).v_a = args.Value
		reply.OK = true
	} else {
		// Reject the proposal
		reply.OK = false
	}
	return nil
}

func (px *Paxos) UpdatePeer2HighestDoneSeq(seq int, instance int) {
	if HighestDoneSeq, exists := px.peer2HighestDoneSeq.Load(instance); exists {
		if seq > HighestDoneSeq.(int) {
			px.peer2HighestDoneSeq.Store(instance, seq)
		}
	}
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.UpdatePeer2HighestDoneSeq(seq, px.me)
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	// Your code here.
	maxseq := math.MinInt

	px.seq2Status.Range(func(seq, status interface{}) bool {
		s, ok := seq.(int)
		if ok && s > maxseq {
			maxseq = s
		}
		return true
	})

	return maxseq
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	// You code here.
	minseq := math.MaxInt
	px.peer2HighestDoneSeq.Range(func(key, value interface{}) bool {
		doneSeq := value.(int)
		if doneSeq < minseq {
			minseq = doneSeq
		}
		return true
	})

	return minseq + 1
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.

	status, exists := px.seq2Status.Load(seq)

	if seq < px.Min() || !exists {
		return Forgotten, nil
	}
	if status.(Fate) == Decided {
		consVal, _ := px.seq2ConsVal.Load(seq)
		return Decided, consVal
	} else {
		return Pending, nil
	}
}

// Delete all Done
func (px *Paxos) ForgetDone(minseq int) {
	px.seq2Status.Range(func(key, value interface{}) bool {
		seq := key.(int)
		if seq < minseq {
			// Remove any state associated with this sequence number
			px.seq2Status.Delete(seq)
			px.seq2PropNum.Delete(seq)
			px.seq2ConsVal.Delete(seq)
			px.seq2AcceptorState.Delete(seq)
		}
		return true
	})
}

// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

// has this peer been asked to shut down?
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.seq2Status = sync.Map{}
	px.seq2PropNum = sync.Map{}
	px.seq2ConsVal = sync.Map{}
	px.seq2AcceptorState = sync.Map{}
	px.peer2HighestDoneSeq = sync.Map{}

	for peer := range px.peers {
		px.peer2HighestDoneSeq.Store(peer, -1)
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}

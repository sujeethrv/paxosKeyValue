package kvpaxos

const (
	OK                      = "OK"
	ErrNoKey                = "ErrNoKey"
	ErrMissingKey           = "ErrMissingKey"
	ErrMissingRequestID2Seq = "ErrMissingRequestID"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq       int
	RequestID int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Seq       int
	RequestID int
}

type GetReply struct {
	Err   Err
	Value string
}

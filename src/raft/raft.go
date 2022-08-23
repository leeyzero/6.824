package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	DefaultHeartbeatInterval = 50 * time.Millisecond
	DefaultElectionTimeout   = 150 * time.Millisecond
)

const (
	Stopped   = "stopped"
	Follower  = "follower"
	Candidate = "candidate"
	Leader    = "leader"
)

var (
	NotLeaderError = errors.New("raft: Not current leader")
	StopError      = errors.New("raft: Has been stopped")
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	LogIndex int
	LogTerm  int
	Command  interface{}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []*LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

type event struct {
	target      interface{}
	returnValue interface{}
	errc        chan error
}

// Global functions
func newRequestVoteArgs(term int, candidateId int, lastLogIndex int, lastLogTerm int) *RequestVoteArgs {
	return &RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm}
}

func newAppendEntriesArgs(term int, leaderId int, prevLogIndex int, prevLogTerm int, leaderCommit int, entries []*LogEntry) *AppendEntriesArgs {
	return &AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, leaderCommit, entries}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	leader      int
	log         []*LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// config
	state string

	// sync control
	stopped chan bool
	c       chan *event
	wg      sync.WaitGroup
}

// return currentTerm and whether this server
// believes it is the leader.
func (r *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return r.CurrentTerm(), r.Me() == r.Leader()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (r *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(r.xxx)
	// e.Encode(r.yyy)
	// data := w.Bytes()
	// r.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (r *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   r.xxx = xxx
	//   r.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (r *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (r *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC handler.
//
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
}

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (r *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := r.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (r *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := r.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (r *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := r.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (r *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (r *Raft) Kill() {
	if r.killed() {
		return
	}

	atomic.StoreInt32(&r.dead, 1)
	// Your code here, if desired.
	r.stopLoop()
}

func (r *Raft) killed() bool {
	z := atomic.LoadInt32(&r.dead)
	return z == 1
}

func (r *Raft) State() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

func (r *Raft) CurrentTerm() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm
}

func (r *Raft) Leader() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leader
}

func (r *Raft) Me() int {
	return r.me
}

func (r *Raft) QuorumSize() int {
	return (len(r.peers) / 2) + 1
}

func (r *Raft) setState(state string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if state == r.state {
		return
	}

	r.state = state
	if state == Leader {
		r.leader = r.me
	}
}

func (r *Raft) send(value interface{}) (interface{}, error) {
	if r.killed() {
		return nil, StopError
	}

	e := &event{target: value, errc: make(chan error, 1)}
	select {
	case <-r.stopped:
		return nil, StopError
	case r.c <- e:
	}
	select {
	case <-r.stopped:
		return nil, StopError
	case err := <-e.errc:
		return e.returnValue, err
	}
}

func (r *Raft) sendAsync(value interface{}) bool {
	if r.killed() {
		return false
	}

	e := &event{target: value, errc: make(chan error, 1)}
	select {
	case r.c <- e:
		return true
	default:
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()

		select {
		case <-r.stopped:
		case r.c <- e:
		}
	}()
	return true
}

func (r *Raft) startLoop() {
	r.setState(Follower)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.loop()
	}()
}

func (r *Raft) stopLoop() {
	close(r.stopped)
	r.setState(Stopped)
	r.wg.Wait()
}

//               ________
//            --|Snapshot|                 timeout
//            |  --------                  ______
// recover    |       ^                   |      |
// snapshot / |       |snapshot           |      |
// higher     |       |                   v      |     recv majority votes
// term       |    --------    timeout    -----------                        -----------
//            |-> |Follower| ----------> | Candidate |--------------------> |  Leader   |
//                 --------               -----------                        -----------
//                    ^          higher term/ |                         higher term |
//                    |            new leader |                                     |
//                    |_______________________|____________________________________ |
func (r *Raft) loop() {
	defer Info("raft.loop.end")

	state := r.State()
	for state != Stopped {
		Info("raft.loop.run %v is state %v at %v", r.me, state, r.CurrentTerm())
		switch state {
		case Follower:
			r.followerLoop()
		case Candidate:
			r.candidateLoop()
		case Leader:
			r.leaderLoop()
		default:
		}
		state = r.State()
	}
}

func (r *Raft) followerLoop() {
	timeoutC := afterBetween(DefaultElectionTimeout, 2*DefaultElectionTimeout)
	for r.State() == Follower {
		var update bool
		select {
		case <-r.stopped:
			r.setState(Stopped)
			return
		case e := <-r.c:
			var err error
			switch req := e.target.(type) {
			case *AppendEntriesArgs:
				e.returnValue, update = r.processAppendEntriesRequest(req)
			case *RequestVoteArgs:
				e.returnValue, update = r.processRequestVoteRequest(req)
			default:
				err = NotLeaderError
			}
			e.errc <- err
		case <-timeoutC:
			update = true
		}

		// Converts to candidate if election timeout elapses without either:
		//   1.Receiving valid AppendEntries RPC, or
		//   2.Granting vote to candidate
		if update {
			timeoutC = afterBetween(DefaultElectionTimeout, 2*DefaultElectionTimeout)
		}
	}
}

func (r *Raft) candidateLoop() {
	doVote := true
	var votesGranted int
	var replyC chan *RequestVoteReply
	var timeoutC <-chan time.Time

	for r.State() == Candidate {
		if doVote {
			// Increment current term, vote for self.
			term := r.voteForSelf()

			// Send RequestVote RPCs to all other servers.
			replyC = make(chan *RequestVoteReply, len(r.peers))
			for peer, _ := range r.peers {
				if peer == r.me {
					continue
				}
				r.broadcastRequstVote(newRequestVoteArgs(term, r.me, 0, 0), replyC)
			}

			// Wait for either:
			//   * Votes received from majority of servers: become leader
			//   * AppendEntries RPC received from new leader: step down.
			//   * Election timeout elapses without election resolution: increment term, start new election
			//   * Discover higher term: step down (§5.1)
			votesGranted = 1
			timeoutC = afterBetween(DefaultElectionTimeout, 2*DefaultElectionTimeout)
			doVote = false
		}

		// If we received enough votes then stop waiting for more votes.
		// And return from the candidate loop
		if votesGranted == r.QuorumSize() {
			Info("raft.candidate.vote.win")
			r.setState(Leader)
			return
		}

		// Collect votes from peers.
		select {
		case <-r.stopped:
			r.setState(Stopped)
			return
		case reply := <-replyC:
			if r.processRequestVoteResponse(reply) {
				Info("raft.candidate.vote.granted: %v", votesGranted)
				votesGranted++
			}
		case e := <-r.c:
			var err error
			switch req := e.target.(type) {
			case *AppendEntriesArgs:
				e.returnValue, _ = r.processAppendEntriesRequest(req)
			case *RequestVoteArgs:
				e.returnValue, _ = r.processRequestVoteRequest(req)
			default:
				err = NotLeaderError
			}
			// Callback to caller
			e.errc <- err
		case <-timeoutC:
			doVote = true
		}
	}
}

func (r *Raft) leaderLoop() {
	ticker := time.NewTicker(DefaultHeartbeatInterval)
	for r.State() == Leader {
		select {
		case <-r.stopped:
			r.setState(Stopped)
			return
		case e := <-r.c:
			var err error
			switch req := e.target.(type) {
			case *AppendEntriesArgs:
				e.returnValue, _ = r.processAppendEntriesRequest(req)
			case *AppendEntriesReply:
				r.processAppendEntriesResponse(req)
			case *RequestVoteArgs:
				e.returnValue, _ = r.processRequestVoteRequest(req)
			}

			// Callback
			e.errc <- err
		case <-ticker.C:
			// TODO: broadcast heartbeat
		}
	}
}

func (r *Raft) voteForSelf() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm++
	r.votedFor = r.me
	return r.currentTerm
}

func (r *Raft) broadcastRequstVote(req *RequestVoteArgs, replyC chan *RequestVoteReply) {
	for peer, _ := range r.peers {
		if peer == r.me {
			continue
		}

		r.wg.Add(1)
		go func(peer int) {
			defer r.wg.Done()

			Info("raft.rv.send: %v -> %v", r.me, peer)
			var reply RequestVoteReply
			if ok := r.sendRequestVote(peer, req, &reply); !ok {
				Warning("raft.rv.recv.failed: %v <- %v", r.me, peer)
				return
			}
			replyC <- &reply
		}(peer)
	}
}

func (r *Raft) broadcastAppendEntries() {
	// TODO: NOT IMPLEMENTED
}

func (r *Raft) processAppendEntriesRequest(req *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	// TODO: NOT IMPLEMENTED
	return nil, false
}

func (r *Raft) processAppendEntriesResponse(resp *AppendEntriesReply) {
	// TODO: NOT IMPLEMENTED
}

func (r *Raft) processRequestVoteRequest(req *RequestVoteArgs) (*RequestVoteReply, bool) {
	// TODO: NOT IMPLEMENTED
	return nil, false
}

func (r *Raft) processRequestVoteResponse(resp *RequestVoteReply) bool {
	// TODO: NOT IMPLEMENTED
	return false
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	r := &Raft{}
	r.peers = peers
	r.persister = persister
	r.me = me

	// Your initialization code here (2A, 2B, 2C).
	r.currentTerm = 0
	r.votedFor = -1
	r.leader = -1
	r.state = Stopped
	r.stopped = make(chan bool)
	r.c = make(chan *event)

	// initialize from state persisted before a crash
	r.readPersist(persister.ReadRaftState())

	// start loop
	r.startLoop()

	return r
}

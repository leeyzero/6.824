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
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	DefaultHeartbeatInterval = 50 * time.Millisecond
	DefaultElectionTimeout   = 250 * time.Millisecond
	MaxLogEntriesPerRequest  = 2000
)

const (
	Stopped   = "stopped"
	Follower  = "follower"
	Candidate = "candidate"
	Leader    = "leader"
)

const (
	VOTED_FOR_NONE = -1
	LEADER_NONE    = -1
)

var ErrNotLeader = errors.New("raft: Not current leader")
var ErrStopped = errors.New("raft: Has been stopped")

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
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
	Term        int
	Success     bool
	LastIndex   int
	CommitIndex int
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

type CommandArgs struct {
	Command interface{}
}

type CommandReply struct {
	Index int
	Term  int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type AppendEntriesReplyEvent struct {
	Peer  int
	Req   *AppendEntriesArgs
	Reply *AppendEntriesReply
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

func newRequestVoteReply(term int, voteGranted bool) *RequestVoteReply {
	return &RequestVoteReply{term, voteGranted}
}

func newAppendEntriesArgs(term int, leaderId int, prevLogIndex int, prevLogTerm int, leaderCommit int, entries []*LogEntry) *AppendEntriesArgs {
	return &AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, leaderCommit, entries}
}

func newAppendEntriesReply(term int, success bool, lastIndex int, commitIndex int) *AppendEntriesReply {
	return &AppendEntriesReply{term, success, lastIndex, commitIndex}
}

func newCommandArgs(command interface{}) *CommandArgs {
	return &CommandArgs{command}
}

func newCommandReply(index int, term int) *CommandReply {
	return &CommandReply{index, term}
}

// A Go object implementing a single Raft peer.
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
	currentTerm int         // latest term server has seen
	votedFor    int         // candidatedId that received vote in current term (or null if none)
	log         []*LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader
	leader      int         // leader identifier

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders

	// for each server, index of the next log entry to send to that server.
	// initialized to leader last log index+1
	nextIndex []int

	// for each server, index of highest log entry known to be replicated on server
	// initialized to 0, increases monotonically
	matchIndex []int

	// raft role, enums: stopped, follower, candidate, leader
	state string

	// apply channel
	applyCh chan ApplyMsg

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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// RequestVote handler request vote RPC.
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Debug("raft.RequestVote: handle RequestVote RPC with args[%+v]", args)

	reply.Term = 0
	reply.VoteGranted = false

	resp, err := r.send(args)
	if err != nil {
		Warning("raft.RequestVote: server[%v] at term[%v] send event err[%v]", r.me, r.CurrentTerm(), err)
		return
	}
	rvReply, ok := resp.(*RequestVoteReply)
	if !ok {
		Warning("raft.RequestVote: server[%v] at term[%v] type assert failed", r.me, r.CurrentTerm())
		return
	}

	reply.Term = rvReply.Term
	reply.VoteGranted = rvReply.VoteGranted
}

// AppendEntries handle append entries RPC
func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Debug("raft.AppendEntries: handle AppendEntries RPC with args[%+v]", args)

	reply.Term = 0
	reply.Success = false

	resp, err := r.send(args)
	if err != nil {
		Warning("raft.AppendEntries: server[%v] at term[%v] send event err[%v]", r.me, r.CurrentTerm(), err)
		return
	}
	aeReply, ok := resp.(*AppendEntriesReply)
	if !ok {
		Warning("raft.AppendEntries: server[%v] at term[%v] type assert failed", r.me, r.CurrentTerm())
		return
	}

	reply.Term = aeReply.Term
	reply.Success = aeReply.Success
	reply.LastIndex = aeReply.LastIndex
	reply.CommitIndex = aeReply.CommitIndex
}

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
func (r *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := 0

	// Your code here (2B).
	resp, err := r.send(newCommandArgs(command))
	if err != nil {
		return index, term, false
	}
	cmdReply, ok := resp.(*CommandReply)
	if !ok {
		return index, term, false
	}

	return cmdReply.Index, cmdReply.Term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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

func (r *Raft) VotedFor() int {
	return r.votedFor
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
		return nil, ErrStopped
	}

	e := &event{target: value, errc: make(chan error, 1)}
	select {
	case <-r.stopped:
		return nil, ErrStopped
	case r.c <- e:
	}
	select {
	case <-r.stopped:
		return nil, ErrStopped
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

	go func() {
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
	Info("raft.stopLoop: server[%v] state[%v] at term[%v] stopped", r.me, r.State(), r.CurrentTerm())
}

//	   ________
//	--|Snapshot|                 timeout
//	|  --------                  ______
//
// recover    |       ^                   |      |
// snapshot / |       |snapshot           |      |
// higher     |       |                   v      |     recv majority votes
// term       |    --------    timeout    -----------                        -----------
//
//	|-> |Follower| ----------> | Candidate |--------------------> |  Leader   |
//	     --------               -----------                        -----------
//	        ^          higher term/ |                         higher term |
//	        |            new leader |                                     |
//	        |_______________________|____________________________________ |
func (r *Raft) loop() {
	state := r.State()
	for state != Stopped {
		Info("raft.loop: server[%v] running state[%v] at term[%v]", r.me, state, r.CurrentTerm())

		switch state {
		case Follower:
			r.followerLoop()
		case Candidate:
			r.candidateLoop()
		case Leader:
			r.leaderLoop()
		default:
			Warning("raft.loop: server[%v] running unknown state[%v] at term[%v]", r.me, state, r.CurrentTerm())
		}
		state = r.State()
	}

	Info("raft.loop: server[%v] at term[%v], stopping...", r.me, r.CurrentTerm())
}

func (r *Raft) followerLoop() {
	timeoutC := afterBetween(DefaultElectionTimeout, 2*DefaultElectionTimeout)
	for r.State() == Follower {
		var update bool
		select {
		case <-r.stopped:
			return
		case e := <-r.c:
			var err error
			switch req := e.target.(type) {
			case *AppendEntriesArgs:
				e.returnValue, update = r.processAppendEntriesRequest(req)
			case *RequestVoteArgs:
				e.returnValue, update = r.processRequestVoteRequest(req)
			default:
				err = ErrNotLeader
			}
			e.errc <- err
		case <-timeoutC:
			r.setState(Candidate)
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
	var votesGranted int
	var replyC chan *RequestVoteReply
	var timeoutC <-chan time.Time
	doVote := true

	for r.State() == Candidate {
		if doVote {
			// Increment current term, vote for self.
			term := r.voteForSelf()

			// Send RequestVote RPCs to all other servers.
			lastLogIndex, lastLogTerm := r.lastLogInfo()
			replyC = r.broadcastRequstVote(newRequestVoteArgs(term, r.me, lastLogIndex, lastLogTerm))

			// Wait for either:
			//   - Votes received from majority of servers: become leader
			//   - AppendEntries RPC received from new leader: step down.
			//   - Election timeout elapses without election resolution: increment term, start new election
			//   - Discover higher term: step down (§5.1)
			votesGranted = 1
			timeoutC = afterBetween(DefaultElectionTimeout, 2*DefaultElectionTimeout)
			doVote = false
		}

		// If we received enough votes then stop waiting for more votes.
		// And return from the candidate loop
		if votesGranted >= r.QuorumSize() {
			Info("raft.candidateLoop: server[%v] win votes at term[%v]", r.me, r.CurrentTerm())
			r.setState(Leader)
			return
		}

		// Collect votes from peers.
		select {
		case <-r.stopped:
			return
		case reply := <-replyC:
			if r.processRequestVoteReply(reply) {
				votesGranted++
				Info("raft.candidateLoop: server[%v] at term[%v] recieved granted votes[%v]", r.me, r.CurrentTerm(), votesGranted)
			}
		case e := <-r.c:
			var err error
			switch req := e.target.(type) {
			case *AppendEntriesArgs:
				e.returnValue, _ = r.processAppendEntriesRequest(req)
			case *RequestVoteArgs:
				e.returnValue, _ = r.processRequestVoteRequest(req)
			default:
				err = ErrNotLeader
			}
			// Callback to caller
			e.errc <- err
		case <-timeoutC:
			Info("raft.candidateLoop: server[%v] at term[%v] elect timeout redo vote", r.me, r.CurrentTerm())
			doVote = true
		}
	}
}

func (r *Raft) leaderLoop() {
	ticker := time.NewTicker(DefaultHeartbeatInterval)
	defer ticker.Stop()

	// After election:
	// Reinitialized the peers nextIndex to leader's lastLogIndex+1
	// Reinitialized the peers matchIndx to 0
	lastLogIndex, _ := r.lastLogInfo()
	for peer := range r.nextIndex {
		r.nextIndex[peer] = lastLogIndex + 1
		r.matchIndex[peer] = 0
	}

	// Once a candidate wins an election, it becomes leader. It then sends heartbeat message to all of the
	// other servers to establish its authority and prevent new elections.
	refreshC := make(chan bool, 1)
	refreshC <- true

	for r.State() == Leader {
		var needBroadcastAppendEntries bool
		select {
		case <-r.stopped:
			return
		case e := <-r.c:
			var err error
			switch req := e.target.(type) {
			case *AppendEntriesArgs:
				e.returnValue, _ = r.processAppendEntriesRequest(req)
			case *AppendEntriesReplyEvent:
				err = r.processAppendEntriesReply(req.Peer, req.Req, req.Reply)
			case *RequestVoteArgs:
				e.returnValue, _ = r.processRequestVoteRequest(req)
			case *CommandArgs:
				e.returnValue, err = r.processCommandRequest(req)
				if err == nil {
					// 主动触发 AE RPC
					needBroadcastAppendEntries = true
					ticker.Reset(DefaultHeartbeatInterval)
				}
			}

			// Callback
			e.errc <- err
		case <-ticker.C:
			needBroadcastAppendEntries = true
		case <-refreshC:
			needBroadcastAppendEntries = true
			ticker.Reset(DefaultHeartbeatInterval)
		}

		// heartbeat broadcast append entries rpc to peers
		if needBroadcastAppendEntries {
			r.broadcastAppendEntries()
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

func (r *Raft) getPrevLogIndex(peer int) int {
	return r.nextIndex[peer] - 1
}

func (r *Raft) broadcastRequstVote(req *RequestVoteArgs) chan *RequestVoteReply {
	replyC := make(chan *RequestVoteReply, len(r.peers))
	for peer := range r.peers {
		if peer == r.me {
			continue
		}
		if r.killed() {
			Info("raft.broadcastRequstVote: server[%v] state[%v] at term[%v] killed", r.me, r.State(), r.CurrentTerm())
			break
		}

		// async send RequestVote RPC
		go func(peer int, req *RequestVoteArgs, replyC chan<- *RequestVoteReply) {
			Debug("raft.broadcastRequstVote: server[%v] -> peer[%v] at term[%v] req[%v]", r.me, peer, r.CurrentTerm(), req)

			var reply RequestVoteReply
			if ok := r.sendRequestVote(peer, req, &reply); !ok {
				Warning("raft.broadcastRequstVote: server[%v] -> peer[%v] at term[%v] timeout", r.me, peer, r.CurrentTerm())
				return
			}

			Debug("raft.broadcastRequstVote: server[%v] <- peer[%v] at term[%v] reply[%v]", r.me, peer, r.CurrentTerm(), reply)
			replyC <- &reply
		}(peer, req, replyC)
	}
	return replyC
}

func (r *Raft) broadcastAppendEntries() {
	for peer := range r.peers {
		if peer == r.me {
			continue
		}
		if r.killed() {
			Debug("raft.broadcastAppendEntries: server[%v] state[%v] at term[%v] killed", r.me, r.State(), r.CurrentTerm())
			break
		}

		prevLogIndex := r.getPrevLogIndex(peer)

		// If last log index >= nextIndex for a follower send AppendEntries RPC with log entries starting at nextIndex
		entries, prevLogTerm := r.getLogEntriesAfter(prevLogIndex, MaxLogEntriesPerRequest)
		req := newAppendEntriesArgs(r.CurrentTerm(), r.leader, prevLogIndex, prevLogTerm, r.commitIndex, entries)

		// async send AppendEntries RPC
		go func(peer int, req *AppendEntriesArgs) {
			Debug("raft.broadcastAppendEntries: server[%v] -> peer[%v] at term[%v] req[%v]", r.me, peer, req.Term, req)

			var reply AppendEntriesReply
			if ok := r.sendAppendEntries(peer, req, &reply); !ok {
				Warning("raft.broadcastAppendEntries: server[%v] -> peer[%v] at term[%v] timeout", r.me, peer, req.Term)
				return
			}

			Debug("raft.broadcastAppendEntries: server[%v] <- peer[%v] at term[%v] reply[%v]", r.me, peer, req.Term, reply)

			// async send event to message center
			target := &AppendEntriesReplyEvent{
				Peer:  peer,
				Req:   req,
				Reply: &reply,
			}
			if !r.sendAsync(target) {
				Warning("raft.broadcastAppendEntries: server[%v] async send event failed", r.me)
			}
		}(peer, req)
	}
}

// processAppendEntriesRequest process the "append entries" rpc request
func (r *Raft) processAppendEntriesRequest(req *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term < r.CurrentTerm() {
		Info("raft.processAppendEntriesRequest: leader[%v] term[%v] staled, server[%v] current term[%v]", req.LeaderId, req.Term, r.me, r.CurrentTerm())
		return newAppendEntriesReply(r.CurrentTerm(), false, r.currentLogIndex(), r.commitIndex), false
	}

	if req.Term > r.CurrentTerm() {
		Info("raft.processAppendEntriesRequest: update term due to leader[%v] term[%v] > server[%v] term[%v]",
			req.LeaderId, req.Term, r.me, r.CurrentTerm())
		r.updateCurrentTerm(req.Term, req.LeaderId)
	} else {
		_assert(r.State() != Leader, "raft.processAppendEntriesRequest: leader[%v] elected at same term[%v]", r.me, r.CurrentTerm())

		// discover new leader, save leader id
		r.leader = req.LeaderId

		// step-down to follower when it is a candidate
		if r.State() == Candidate {
			r.setState(Follower)
		}
	}

	// Reject if log doesn't contain a matching previous entry.
	if err := r.truncateLog(req.PrevLogIndex, req.PrevLogTerm); err != nil {
		Info("raft.processAppendEntriesRequest: server[%v] truncateLog[%v, %v] err[%v]", r.me, req.PrevLogIndex, req.PrevLogTerm, err)
		return newAppendEntriesReply(r.CurrentTerm(), false, r.currentLogIndex(), r.commitIndex), true
	}

	// Append entries to the log.
	if err := r.appendLogEntries(req.Entries); err != nil {
		Info("raft.processAppendEntriesRequest: server[%v] appendLogEntries[%v] err[%v]", r.me, len(req.Entries), err)
		return newAppendEntriesReply(r.CurrentTerm(), false, r.currentLogIndex(), r.commitIndex), true
	}

	// Commit up to the commit index.
	if err := r.setCommitIndex(req.LeaderCommit); err != nil {
		Info("raft.processAppendEntriesRequest: server[%v] setCommitIndex[%v] err[%v]", r.me, req.LeaderCommit, err)
		return newAppendEntriesReply(r.CurrentTerm(), false, r.currentLogIndex(), r.commitIndex), true
	}

	// Once the server appended and committed all the log entries from the leader
	return newAppendEntriesReply(r.CurrentTerm(), true, r.currentLogIndex(), r.commitIndex), true
}

// processAppendEntriesReply process the append entries rpc reply
func (r *Raft) processAppendEntriesReply(peer int, req *AppendEntriesArgs, reply *AppendEntriesReply) error {
	if reply.Term > r.CurrentTerm() {
		Info("raft.processAppendEntriesReply: update term due to peer[%v] term[%v] > server[%v] current term[%v]",
			peer, reply.Term, r.me, r.CurrentTerm())
		r.updateCurrentTerm(reply.Term, LEADER_NONE)
		return nil
	}

	if reply.Success {
		// If success: update nextIndex and matchIndex for follower
		if len(req.Entries) > 0 {
			r.nextIndex[peer] = req.Entries[len(req.Entries)-1].Index + 1
			r.matchIndex[peer] = req.Entries[len(req.Entries)-1].Index

			// If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N
			// and log[N].Term == currentTerm: set commitIndex = N (§5.3, §5.4)
			matches := make([]int, len(r.matchIndex))
			copy(matches, r.matchIndex)
			sort.Sort(sort.Reverse(sort.IntSlice(matches)))
			commitIndex := matches[r.QuorumSize()-1]
			if commitIndex > r.commitIndex {
				r.setCommitIndex(commitIndex)
			}
		}
	} else if reply.Term == r.CurrentTerm() {
		Info("raft.processAppendEntriesReply: server[%v] state[%v] at term[%v] recieved peer[%v] reply[%v] failed",
			r.me, r.State(), r.CurrentTerm(), peer, reply)

		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry(§5.3)
		if reply.CommitIndex >= r.getPrevLogIndex(peer) {
			r.nextIndex[peer] = reply.CommitIndex + 1
		} else if r.getPrevLogIndex(peer) > 0 {
			r.nextIndex[peer]--
		}
	}
	return nil
}

// processRequestVoteRequest process the "request vote" rpc request
func (r *Raft) processRequestVoteRequest(req *RequestVoteArgs) (*RequestVoteReply, bool) {
	// If the request is coming from an old term then reject it.
	if req.Term < r.CurrentTerm() {
		Info("raft.processRequestVoteRequest: deny vote due to term[%v] less than current term[%v]", req.Term, r.CurrentTerm())
		return newRequestVoteReply(r.CurrentTerm(), false), false
	}

	// If the term of the request peer is larger than this node, update the term and convert role to follower
	// If the term is equal and we've already voted for a different candidate then
	// don't vote for this candidate
	if req.Term > r.CurrentTerm() {
		Info("raft.processRequestVoteRequest: server[%v] update current term[%v] to new term[%v]", r.me, r.CurrentTerm(), req.Term)
		r.updateCurrentTerm(req.Term, LEADER_NONE)
	} else if r.votedFor != VOTED_FOR_NONE && r.votedFor != req.CandidateId {
		Info("raft.processRequestVoteRequest: server[%v] already voted for[%v] at term[%v]", r.me, r.votedFor, r.CurrentTerm())
		return newRequestVoteReply(r.CurrentTerm(), false), false
	}

	// If the candidate's log is not at least as up-to-date as our last log then don't vote.
	// two logs is more up-to-date defines(§5.3.1):
	// If the logs have last enries with different terms, then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer is more up-to-date.
	lastLogIndex, lastLogTerm := r.lastLogInfo()
	if lastLogTerm > req.LastLogTerm || (lastLogTerm == req.LastLogTerm && lastLogIndex > req.LastLogIndex) {
		Info("raft.processRequestVoteRequest: server[%v] at term[%v] with last log[%v %v] deny vote for candidate req[%v]",
			r.me, r.CurrentTerm(), lastLogIndex, lastLogTerm, req)
		return newRequestVoteReply(r.CurrentTerm(), false), false
	}

	Debug("raft.processRequestVoteRequest: server[%v] vote for[%v] at term[%v]", r.me, req.CandidateId, r.CurrentTerm())

	// If we made it this far then cast a vote and reset our election time out.
	r.votedFor = req.CandidateId
	return newRequestVoteReply(r.CurrentTerm(), true), true
}

// processVoteReply processes a vote request:
//  1. if the vote is granted for the current term of the candidate, return true
//  2. if the vote is denied due to smaller term, update the term of this server
//     which will also cause the candidate to step-down, and return false.
//  3. if the vote is for a smaller term, ignore it and return false.
func (r *Raft) processRequestVoteReply(reply *RequestVoteReply) bool {
	if reply.VoteGranted && reply.Term == r.CurrentTerm() {
		return true
	}
	if reply.Term > r.CurrentTerm() {
		r.updateCurrentTerm(reply.Term, LEADER_NONE)
	}

	Debug("raft.processRequestVoteReply: vote failed due to peer term[%v] not equal current term[%v] or election restriction", reply.Term, r.CurrentTerm())
	return false
}

// processCommandRequest process command request
func (r *Raft) processCommandRequest(req *CommandArgs) (*CommandReply, error) {
	entry := r.createLogEntry(req.Command)
	if err := r.appendLogEntry(entry); err != nil {
		Warning("raft.processCommandRequest: server[%v] append log entry[%v] err[%v]", r.me, entry, err)
		return nil, err
	}

	// leader已完成复制，更新nextIndex和matchIndex
	r.nextIndex[r.me] = entry.Index + 1
	r.matchIndex[r.me] = entry.Index

	Debug("raft.processCommandRequest: server[%v] append log entry[%v] success, log len[%v]", r.me, entry, len(r.log))
	return newCommandReply(entry.Index, entry.Term), nil
}

func (r *Raft) updateCurrentTerm(term int, leader int) {
	_assert(term > r.CurrentTerm(), "updated term MUST be larger than current term")

	r.mu.Lock()
	defer r.mu.Unlock()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower(§5.1)
	r.currentTerm = term
	r.state = Follower
	r.leader = leader
	r.votedFor = VOTED_FOR_NONE
}

// lastLogInfo return last log index and term
func (r *Raft) lastLogInfo() (int, int) {
	if len(r.log) == 0 {
		return 0, 0
	}

	lastEntry := r.log[len(r.log)-1]
	return lastEntry.Index, lastEntry.Term
}

// create a new log entry
func (r *Raft) createLogEntry(command interface{}) *LogEntry {
	return &LogEntry{
		Index:   r.nextLogIndex(),
		Term:    r.CurrentTerm(),
		Command: command,
	}
}

func (r *Raft) currentLogIndex() int {
	if len(r.log) == 0 {
		return 0
	}
	return r.log[len(r.log)-1].Index
}

func (r *Raft) nextLogIndex() int {
	return r.currentLogIndex() + 1
}

func (r *Raft) appendLogEntries(entries []*LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	for _, entry := range entries {
		if err := r.appendLogEntry(entry); err != nil {
			return err
		}
	}
	return nil
}

func (r *Raft) appendLogEntry(entry *LogEntry) error {
	// Make sure the term and index are greater than the previous.
	if len(r.log) > 0 {
		lastEntry := r.log[len(r.log)-1]
		if entry.Term < lastEntry.Term {
			return fmt.Errorf("cannot append entry with earlier term")
		} else if entry.Term == lastEntry.Term && entry.Index <= lastEntry.Index {
			return fmt.Errorf("cannot append entry with earlier idnex")
		}
	}

	// append to entry
	r.log = append(r.log, entry)
	return nil
}

// Retrieves a list of entries after a given index as well as the term of the
// index provided.
func (r *Raft) getLogEntriesAfter(index int, maxLogEntriesPerRequest int) ([]*LogEntry, int) {
	// return nil if index is before the start of the log.
	if index < 0 {
		return nil, 0
	}

	// panic if the index doesn't exist.
	if index > len(r.log) {
		panic(fmt.Sprintf("raft.getLogEntriesAfter: index[%v] is out of log len[%v]", index, len(r.log)))
	}

	if index == 0 {
		return r.log, 0
	}

	targetEntry := r.log[index-1]
	afterEntries := r.log[index:]
	if len(afterEntries) < maxLogEntriesPerRequest {
		return afterEntries, targetEntry.Term
	}
	return afterEntries[:maxLogEntriesPerRequest], targetEntry.Term
}

// Truncates the log to the given index and term. this only works if the log
// at the index has not been committed.
func (r *Raft) truncateLog(index int, term int) error {
	// do not truncated past end of log entries
	if index < 0 || index > len(r.log) {
		return fmt.Errorf("index[%v] with term[%v] does not exist", index, term)
	}

	// truncate all
	if index == 0 {
		r.log = []*LogEntry{}
		return nil
	}

	// do not allow committed log entries to be truncated
	if index < r.commitIndex {
		return fmt.Errorf("index[%v] less than commmitted index[%v]", index, r.commitIndex)
	}

	// Do not truncate if the entry at index does not have the matching term.
	lastEntry := r.log[index-1]
	if lastEntry.Term != term {
		return fmt.Errorf("entry[%v %v] not match target term[%v]", index, term, lastEntry.Term)
	}

	// otherwise truncate the disired entry.
	r.log = r.log[:index]
	return nil
}

func (r *Raft) setCommitIndex(leaderCommit int) error {
	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if leaderCommit > len(r.log) {
		leaderCommit = len(r.log)
	}

	// do not allow previous indices to be committed again.
	if leaderCommit <= r.commitIndex {
		return nil
	}

	Debug("raft.setCommitIndex: server[%v] at term[%v] commit index from[%v] to[%v]",
		r.me, r.CurrentTerm(), r.commitIndex+1, leaderCommit)

	for i := r.commitIndex + 1; i <= leaderCommit; i++ {
		entry := r.log[i-1]
		r.commitIndex = entry.Index

		// apply the changes to the state machine
		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		}
		r.applyCh <- msg

		// update lastApplied
		r.lastApplied = r.commitIndex
	}
	return nil
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	r := &Raft{}
	r.peers = peers
	r.persister = persister
	r.me = me
	r.dead = 0

	// Your initialization code here (2A, 2B, 2C).
	r.currentTerm = 0
	r.votedFor = VOTED_FOR_NONE
	r.log = make([]*LogEntry, 0)

	r.leader = LEADER_NONE
	r.state = Stopped
	r.applyCh = applyCh

	r.stopped = make(chan bool)
	r.c = make(chan *event)

	// initialize from state persisted before a crash
	r.readPersist(persister.ReadRaftState())

	// initialize matchIndex
	r.matchIndex = make([]int, len(peers))

	// initialize nextIndex
	r.nextIndex = make([]int, len(peers))

	// start loop
	r.startLoop()

	return r
}

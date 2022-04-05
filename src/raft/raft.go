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
	"6.824/labgob"
	"bytes"
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Follower int = iota
	Candidate
	Leader
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
	Term    int
	Command interface{}
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

	// Server states
	state int

	// Persistent state
	currentTerm       int
	votedFor          int
	lastIncludedTerm  int
	lastIncludedIndex int
	logs              []LogEntry

	// Volatile state
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// election timeout
	electionTimeOut   time.Duration
	isElectionTimeOut bool

	heartbeatTime time.Duration

	// for cpu optimization
	reSetElectionTimerCond *sync.Cond

	// applier conditional variable
	applierCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) persistEncode() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.logs)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	data := rf.persistEncode()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var lastIncludedTerm int
	var lastIncludedIndex int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&logs) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastIncludedIndex = lastIncludedIndex
		rf.logs = logs
	}
}

func (rf *Raft) logLen() int {
	l := len(rf.logs)
	if rf.lastIncludedIndex == 0 {
		return l
	}
	return rf.lastIncludedIndex + l + 1
}

// trim log from header include index
func (rf *Raft) logTrimHead(index int) {
	l := len(rf.logs)
	if rf.lastIncludedIndex > 0 {
		index = index - rf.lastIncludedIndex
	} else {
		index = index + 1
	}
	copy(rf.logs, rf.logs[index:])
	rf.logs = rf.logs[:l-index]
}

// trim log from tail include index
func (rf *Raft) logTrimTail(index int) {
	if rf.lastIncludedIndex > 0 {
		rf.logs = rf.logs[:index-rf.lastIncludedIndex-1]
	} else {
		rf.logs = rf.logs[:index]
	}
}

func (rf *Raft) logAtIndex(index int) LogEntry {
	if rf.lastIncludedIndex > 0 {
		return rf.logs[index-rf.lastIncludedIndex-1]
	} else {
		return rf.logs[index]
	}
}

func (rf *Raft) logTermAtIndex(index int) int {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	} else {
		return rf.logAtIndex(index).Term
	}
}

func (rf *Raft) logRealIndex(index int) int {
	if rf.lastIncludedIndex > 0 {
		return index + rf.lastIncludedIndex + 1
	} else {
		return index
	}
}

// first log with term
func (rf *Raft) logFirstWithTerm(term int) int {
	index := 0
	for i, log := range rf.logs {
		if log.Term == term {
			index = i
			break
		}
	}
	if rf.lastIncludedIndex > 0 {
		return index + rf.lastIncludedIndex + 1
	}
	return index
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.lastIncludedIndex {
		rf.lastIncludedTerm = rf.logTermAtIndex(index)
		rf.logTrimHead(index)
		rf.lastIncludedIndex = index
		data := rf.persistEncode()
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	}
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// reset election only when granted
	isLogUpToDate := rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		if isLogUpToDate {
			rf.isElectionTimeOut = false
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
		rf.persist()
		rf.reSetElectionTimerCond.Broadcast()
	} else if isLogUpToDate {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.votedFor = args.CandidateId
			rf.state = Follower
			rf.isElectionTimeOut = false
			reply.VoteGranted = true
			rf.persist()
			rf.reSetElectionTimerCond.Broadcast()
		}
	}
	return
}

func (rf *Raft) isLogUpToDate(LastLogTerm int, LastLogIndex int) bool {
	lastLogIndex := rf.logLen() - 1
	lastLogTerm := rf.logTermAtIndex(lastLogIndex)
	return LastLogTerm > lastLogTerm || LastLogTerm == lastLogTerm && LastLogIndex >= lastLogIndex
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// for optimization
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// reset election timer
	rf.state = Follower
	rf.isElectionTimeOut = false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.reSetElectionTimerCond.Broadcast()

	defer rf.persist()

	// avoid log move backward,receiver shoud do deal with this case
	if args.PrevLogIndex < rf.lastIncludedIndex {
		return
	}

	l := rf.logLen()
	if l-1 < args.PrevLogIndex {
		reply.ConflictIndex = l
		reply.ConflictTerm = 0
	} else {
		if rf.logTermAtIndex(args.PrevLogIndex) != args.PrevLogTerm {
			reply.ConflictTerm = rf.logTermAtIndex(args.PrevLogIndex)
			reply.ConflictIndex = rf.logFirstWithTerm(reply.ConflictTerm)
			// delete conflict log
			rf.logTrimTail(args.PrevLogIndex)
		} else {
			for i := range args.Entries {
				if l-1 < args.PrevLogIndex+i+1 {
					rf.logs = append(rf.logs, args.Entries[i])
				}
				if rf.logTermAtIndex(args.PrevLogIndex+i+1) != args.Entries[i].Term {
					// delete conflict log
					rf.logTrimTail(args.PrevLogIndex + i + 1)
					rf.logs = append(rf.logs, args.Entries[i])
					l = rf.logLen()
				}
			}
			reply.Success = true
			// update commitIndex
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
				// broadcast to applier
				rf.applierCond.Broadcast()
			}
		}
	}
}

func min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapShotReply struct {
	Term int
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if rf.commitIndex >= args.LastIncludedIndex {
		return
	}
	var found bool
	for i, log := range rf.logs {
		if log.Term == args.LastIncludedTerm && rf.logRealIndex(i) == args.LastIncludedIndex {
			found = true
			break
		}
	}

	// accept snapshot
	if !found {
		rf.logs = rf.logs[:0]
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.commitIndex = args.LastIncludedIndex
		rf.applierCond.Broadcast()
	}

	// reset election timer
	rf.state = Follower
	rf.isElectionTimeOut = false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.reSetElectionTimerCond.Broadcast()

	data := rf.persistEncode()
	rf.persister.SaveStateAndSnapshot(data, args.Data)
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.state == Leader
	term = rf.currentTerm
	if isLeader {
		index = rf.logLen()
		logEntry := LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.logs = append(rf.logs, logEntry)
		// send append entry rpc
		rf.persist()
		rf.heartbeatOrAppendEntries()
	}
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
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		for rf.state != Follower {
			rf.reSetElectionTimerCond.Wait()
		}

		rf.isElectionTimeOut = true
		rf.mu.Unlock()
		time.Sleep(rf.electionTimeOut)
		rf.mu.Lock()
		// transition to candidate
		if rf.state == Follower && rf.isElectionTimeOut {
			rf.currentTerm++
			rf.state = Candidate
			rf.sendVote()
			go rf.startElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	for rf.killed() == false {
		time.Sleep(rf.electionTimeOut)
		rf.mu.Lock()
		if rf.state != Candidate {
			rf.mu.Unlock()
			return
		}
		rf.currentTerm++
		rf.sendVote()
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendVote() {
	// vote for self
	rf.votedFor = rf.me
	voteGrantedNum := 1
	rf.persist()
	// send request vote rpc
	currentTerm := rf.currentTerm
	lastLogIndex := rf.logLen() - 1
	lastLogTerm := rf.logTermAtIndex(lastLogIndex)

	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				args := RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := RequestVoteReply{}
				succ := rf.sendRequestVote(server, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if succ {
					if rf.currentTerm != currentTerm {
						return
					}
					if reply.VoteGranted {
						voteGrantedNum++
						if voteGrantedNum > len(rf.peers)/2 {
							if rf.state == Candidate {
								// transition to leader
								rf.state = Leader
								nextIndex := rf.logLen()
								for i := range rf.peers {
									rf.nextIndex[i] = nextIndex
									rf.matchIndex[i] = 0
								}
								rf.heartbeatOrAppendEntries()
								go rf.heartbeat()
							}
						}
					} else {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.state = Follower
							rf.persist()
							rf.reSetElectionTimerCond.Broadcast()
						}
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		time.Sleep(rf.heartbeatTime)
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.heartbeatOrAppendEntries()
		rf.mu.Unlock()
	}
}

func (rf *Raft) AppendEntriesArgs(server int) AppendEntriesArgs {
	nextIndex := rf.nextIndex[server]
	prevLogIndex := nextIndex - 1
	prevLogTerm := rf.logTermAtIndex(prevLogIndex)
	logs := []LogEntry{}
	if rf.lastIncludedIndex > 0 {
		logs = rf.logs[nextIndex-rf.lastIncludedIndex-1:]
	} else {
		logs = rf.logs[nextIndex:]
	}
	// avoid data race
	entries := make([]LogEntry, len(logs))
	copy(entries, logs)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) InstallSnapShotArgs() InstallSnapShotArgs {
	return InstallSnapShotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
}

func (rf *Raft) heartbeatOrAppendEntries() {
	for i := range rf.peers {
		if i != rf.me {
			currentTerm := rf.currentTerm
			if rf.lastIncludedIndex > 0 && rf.nextIndex[i] <= rf.lastIncludedIndex {
				args := rf.InstallSnapShotArgs()
				go func(server int) {
					reply := InstallSnapShotReply{}
					succ := rf.sendInstallSnapShot(server, &args, &reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if succ {
						if currentTerm != rf.currentTerm {
							return
						}
						if reply.Term > rf.currentTerm {
							rf.votedFor = -1
							rf.currentTerm = reply.Term
							rf.isElectionTimeOut = false
							rf.state = Follower
							rf.persist()
							rf.reSetElectionTimerCond.Broadcast()
							return
						}
						if rf.matchIndex[server] < args.LastIncludedIndex {
							rf.matchIndex[server] = args.LastIncludedIndex
							rf.nextIndex[server] = args.LastIncludedIndex + 1
							rf.updateLeaderCommitIndex()
						}
					}
				}(i)
			} else {
				// construct rpc args
				args := rf.AppendEntriesArgs(i)
				// send rpc
				go func(server int) {
					reply := AppendEntriesReply{}
					succ := rf.sendAppendEntries(server, &args, &reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if succ {
						if currentTerm != rf.currentTerm {
							return
						}
						if !reply.Success && reply.Term > rf.currentTerm {
							rf.votedFor = -1
							rf.currentTerm = reply.Term
							rf.isElectionTimeOut = false
							rf.state = Follower
							rf.persist()
							rf.reSetElectionTimerCond.Broadcast()
						} else if !reply.Success && (reply.ConflictTerm > 0 || reply.ConflictIndex > 0) {
							var found bool
							if reply.ConflictTerm > 0 {
								for i := range rf.logs {
									if rf.logs[i].Term == reply.ConflictTerm {
										found = true
									}
									// the first one beyound conflict term
									// it must exist
									if found {
										if rf.logs[i].Term != reply.ConflictTerm {
											rf.nextIndex[server] = rf.logRealIndex(i)
											break
										}
									}
								}
							}
							if !found {
								rf.nextIndex[server] = reply.ConflictIndex
							}
						} else if reply.Success {
							// update nextIndex,matchIndex,commitIndex
							matchIndex := args.PrevLogIndex + len(args.Entries)
							if matchIndex+1 > rf.nextIndex[server] {
								rf.nextIndex[server] = matchIndex + 1
								rf.matchIndex[server] = matchIndex
							}
							if len(args.Entries) > 0 {
								rf.updateLeaderCommitIndex()
							}
						}
					}
				}(i)
			}
		}
	}
}

func (rf *Raft) updateLeaderCommitIndex() {
	rf.matchIndex[rf.me] = rf.logLen() - 1
	matchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex)
	quorum := len(rf.peers) / 2
	i := matchIndex[quorum]
	// only commit current term log
	if i > rf.commitIndex && rf.logTermAtIndex(i) == rf.currentTerm {
		rf.commitIndex = i
	}
	// broadcast to applier
	rf.applierCond.Broadcast()
}

func (rf *Raft) applier(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applierCond.Wait()
		}
		if rf.lastApplied >= rf.lastIncludedIndex {
			lastApplied := rf.lastApplied + 1
			log := rf.logAtIndex(lastApplied)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: lastApplied,
			}
			rf.mu.Unlock()
			applyCh <- msg
			rf.mu.Lock()
			if lastApplied > rf.lastApplied {
				rf.lastApplied = lastApplied
			}
			rf.mu.Unlock()
		} else {
			lastApplied := rf.lastIncludedIndex
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.persister.ReadSnapshot(),
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.mu.Unlock()
			applyCh <- msg
			rf.mu.Lock()
			if lastApplied > rf.lastApplied {
				rf.lastApplied = lastApplied
			}
			rf.mu.Unlock()
		}
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastIncludedTerm = 0
	rf.lastIncludedIndex = 0
	rf.logs = []LogEntry{{}} // 0 as dummy

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.electionTimeOut = time.Duration(rand.Intn(200)+300) * time.Millisecond
	rf.heartbeatTime = time.Millisecond * 100

	rf.reSetElectionTimerCond = sync.NewCond(&rf.mu)

	rf.applierCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier(applyCh)
	return rf
}

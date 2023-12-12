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
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

// Status represents a Raft server is a LEADER or a CANDIDATE or a FOLLOWER
type Status int

const (
	LEADER = iota
	CANDIDATE
	FOLLOWER
)

// Timeouts (ms)
const TIMEOUT_MIN = 300
const TIMEOUT_RANGE = 150
const TIMEOUT_HEART_BEAT = 90

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/// persistent state on all servers

	// latest term server has seen (initialized to 0
	// on first boot, increases monotonically)
	currentTerm int
	// candidateId that received vote in current term
	// (or null if none)
	votedFor int
	// log entries; each entry contains command
	// for state machine, and term when entry
	// was received by leader (first index is 1)
	logs []LogEntry

	/// volatile state on all servers

	// index of highest log entry known to be
	// committed (initialized to 0, increases
	// monotonically)
	commitIndex int
	// index of highest log entry applied to state
	// machine (initialized to 0, increases
	// monotonically)
	lastApplied int

	/// volatile state on leaders:

	//  for each server, index of the next log entry
	//	to send to that server (initialized to leader
	//	last log index + 1)
	nextIndex []int
	//  for each server, index of highest log entry
	//  known to be replicated on server
	// (initialized to 0, increases monotonically)
	matchIndex []int

	// status \in \{ LEADER, CANDIDATE, FOLLOWER \}
	status Status
	// timer for vote or heartbeat
	timer *time.Timer
	// send applyCh to service (or tester) when successive log entries
	applyCh chan ApplyMsg
	// number of votes gained when being a candidate
	votes int
	// signal used to reset timer
	resetTimer chan bool
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArguments struct {
	// leader's term
	Term int
	// so follower can redirect clients
	LeaderId int
	// index of log entry immediately preceding
	// new ones
	PrevLogIndex int
	// term of prevLogIndex entry
	PrevLogTerm int
	// log entries to store (empty for heartbeat;
	// may send more than one for efficiency)
	Entries []LogEntry
	// leader’s commitIndex
	LeaderCommit int
}

type AppendEntriesResults struct {
	// currentTerm, for leader to update itself
	Term int
	// true if follower contained entry matching
	// prevLogIndex and prevLogTerm
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.status == LEADER

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

func (rf *Raft) setHeartbeatTimer() {
	rf.timer = time.NewTimer(time.Duration(TIMEOUT_HEART_BEAT) * time.Millisecond)
}

func (rf *Raft) setVoteTimer() {
	rf.timer = time.NewTimer(time.Duration(TIMEOUT_MIN+rand.Intn(TIMEOUT_RANGE)) * time.Millisecond)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.

	// candidate’s term
	Term int
	// candidate requesting vote
	CandidateId int
	// index of candidate’s last log entry
	LastLogIndex int
	// term of candidate’s last log entry
	LastLogTerm int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.

	// currentTerm, for candidate to update itself
	Term int
	// true means candidate received vote
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Receiver implementation:
	//1. Reply false if term < currentTerm (§5.1)
	//2. If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		rf.beFollower()
	}

	reply.Term = rf.currentTerm

	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()

		if rf.status == FOLLOWER {
			rf.resetTimer <- true
		}
	} else {
		reply.VoteGranted = false
	}

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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok && rf.status == CANDIDATE {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower (§5.1)
		if rf.currentTerm < reply.Term {
			rf.votedFor = -1
			rf.currentTerm = reply.Term

			rf.persist()
			rf.beFollower()
		} else if reply.VoteGranted == true && reply.Term == rf.currentTerm {
			// If votes received from majority of servers: become leader
			rf.votes += 1

			if rf.votes > len(rf.peers)/2 {
				rf.beLeader()
			}
		}
	}

	return ok
}

// AppendEntries
// / 1. Reply false if term < currentTerm (§5.1)
// / 2. Reply false if log doesn’t contain an entry at prevLogIndex
// / whose term matches prevLogTerm (§5.3)
// / 3. If an existing entry conflicts with a new one (same index
// / but different terms), delete the existing entry and all that
// / follow it (§5.3)
// / 4. Append any new entries not already in the log
// / 5. If leaderCommit > commitIndex, set commitIndex =
// / min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(arguments *AppendEntriesArguments, results *AppendEntriesResults) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if rf.status == FOLLOWER {
		if arguments.Term > rf.currentTerm {
			rf.currentTerm = arguments.Term
			rf.persist()
		}
	} else if rf.status == CANDIDATE {
		// If AppendEntries RPC received from new leader: convert to
		// follower
		if arguments.Term >= rf.currentTerm {
			rf.currentTerm = arguments.Term
			rf.votedFor = -1
			rf.beFollower()
		}
	}

	// Reply false if term < currentTerm (§5.1)
	if arguments.Term < rf.currentTerm {
		results.Term = rf.currentTerm
		results.Success = false
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if arguments.PrevLogIndex < 0 || arguments.PrevLogIndex >= len(rf.logs) || arguments.PrevLogTerm != rf.logs[arguments.PrevLogIndex].Term {
		results.Term = rf.currentTerm
		results.Success = false

		if rf.status == FOLLOWER {
			rf.resetTimer <- true
		}

		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if arguments.PrevLogIndex+1 < len(rf.logs) {
		rf.logs = rf.logs[:arguments.PrevLogIndex+1]
	}

	// Append any new entries not already in the log
	rf.logs = append(rf.logs, arguments.Entries...)

	rf.persist()

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if arguments.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(arguments.LeaderCommit, len(rf.logs)-1)
		// If commitIndex > lastApplied: increment lastApplied, apply
		// log[lastApplied] to state machine (§5.3)
		rf.doApplyMsg()
	}

	results.Term = rf.currentTerm
	results.Success = true

	if rf.status == FOLLOWER {
		rf.resetTimer <- true
	}
}

// Send heartbeat or entries to other servers.
func (rf *Raft) leaderSendHeartbeats(empty bool) {
	for i := range rf.peers {
		if i != rf.me {
			entries := make([]LogEntry, 0)
			if empty == false && len(rf.logs)-1 >= rf.nextIndex[i] {
				entries = rf.logs[rf.nextIndex[i]:]
			}

			go rf.leaderSendAppendEntries(
				i,
				&AppendEntriesArguments{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				},
				&AppendEntriesResults{},
			)
		}
	}

	rf.setHeartbeatTimer()
}

func (rf *Raft) leaderSendAppendEntries(server int, arguments *AppendEntriesArguments, results *AppendEntriesResults) {
	ok := rf.peers[server].Call("Raft.AppendEntries", arguments, results)

	if ok == true {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.status == LEADER {
			if results.Success == true {
				// If successful: update nextIndex and matchIndex for
				// follower (§5.3)
				rf.nextIndex[server] = rf.nextIndex[server] + len(arguments.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1

				// If there exists an N such that N > commitIndex, a majority
				// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
				// set commitIndex = N (§5.3, §5.4).
				for n := len(rf.logs) - 1; n > rf.commitIndex; n-- {
					if rf.currentTerm == rf.logs[n].Term {
						count := 1
						for i := 0; i < len(rf.peers); i++ {
							if i != rf.me && rf.matchIndex[i] >= n {
								count += 1
							}
						}
						if count > len(rf.peers)/2 {
							rf.commitIndex = n
							break
						}
					}
				}
				// If commitIndex > lastApplied: increment lastApplied, apply
				// log[lastApplied] to state machine (§5.3)
				rf.doApplyMsg()
			} else if rf.currentTerm >= results.Term {
				// If AppendEntries fails because of log inconsistency:
				// decrement nextIndex and retry (§5.3)
				rf.nextIndex[server] -= 1
			} else if rf.currentTerm < results.Term {
				// If RPC request or response contains term T > currentTerm:
				// set currentTerm = T, convert to follower (§5.1)
				rf.currentTerm = results.Term
				rf.votedFor = -1
				rf.persist()
				rf.beFollower()
			}
		}
	}
}

// If commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine (§5.3)
func (rf *Raft) doApplyMsg() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1

		rf.applyCh <- ApplyMsg{
			Index:   rf.lastApplied,
			Command: rf.logs[rf.lastApplied].Command,
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.status == LEADER

	if isLeader == true {
		rf.logs = append(
			rf.logs,
			LogEntry{
				Term:    term,
				Command: command,
			},
		)

		rf.persist()

		index = len(rf.logs) - 1
	}

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{
		Term:    -1,
		Command: nil,
	})
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.status = FOLLOWER
	rf.setVoteTimer()
	rf.resetTimer = make(chan bool, 1)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start concurrent state machine
	go rf.run()

	return rf
}

func (rf *Raft) beFollower() {
	if rf.status != FOLLOWER {
		rf.status = FOLLOWER
		rf.votedFor = -1
		rf.votes = 0
		rf.setVoteTimer()
		rf.resetTimer = make(chan bool, 1)
		rf.persist()
		go rf.run()
	}
}

func (rf *Raft) beCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.status = CANDIDATE
	rf.resetTimer = make(chan bool, 1)
}

func (rf *Raft) beLeader() {
	if rf.status != LEADER {
		rf.status = LEADER

		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))

		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.logs)
			rf.matchIndex[i] = 0
		}
	}

	go rf.run()
}

func (rf *Raft) run() {
	switch rf.status {
	case LEADER:
		rf.mu.Lock()
		if rf.status == LEADER {
			// Upon election: send initial empty AppendEntries RPCs
			// (heartbeat) to each server; repeat during idle periods to
			// prevent election timeouts (§5.2)
			rf.leaderSendHeartbeats(true)
		}
		rf.mu.Unlock()

		// waiting heartbeat response
		if rf.status == LEADER {
			select {
			case <-rf.timer.C:
			}
		}

		// If last log index ≥ nextIndex for a follower: send
		// AppendEntries RPC with log entries starting at nextIndex
		for rf.status == LEADER {
			rf.mu.Lock()

			if rf.status == LEADER {
				rf.leaderSendHeartbeats(false)
				rf.mu.Unlock()

				// waiting heartbeat response
				if rf.status == LEADER {
					select {
					case <-rf.timer.C:
					}
				}
			} else {
				rf.mu.Unlock()
				return
			}
		}
	case FOLLOWER:
		for rf.status == FOLLOWER {
			select {
			case reset := <-rf.resetTimer:
				if reset {
					rf.setVoteTimer()
				}
			case <-rf.timer.C:
				// If election timeout elapses without receiving AppendEntries
				// RPC from current leader or granting vote to candidate:
				// convert to candidate
				rf.beCandidate()
				go rf.run()
			}
		}
	case CANDIDATE:
		for rf.status == CANDIDATE {
			rf.mu.Lock()

			if rf.status == CANDIDATE {
				// On conversion to candidate, start election:
				// * Increment currentTerm
				// * Vote for self
				// * Reset election timer
				// * Send RequestVote RPCs to all other servers
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.votes = 1
				rf.setVoteTimer()

				rf.persist()

				for i := range rf.peers {
					if i != rf.me {
						go rf.sendRequestVote(
							i,
							&RequestVoteArgs{
								Term:         rf.currentTerm,
								CandidateId:  rf.me,
								LastLogIndex: len(rf.logs) - 1,
								LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
							},
							&RequestVoteReply{},
						)
					}
				}
				rf.mu.Unlock()

				// waiting votes
				if rf.status == CANDIDATE {
					select {
					case <-rf.timer.C:
					}
				}
			} else {
				rf.mu.Unlock()
			}
		}
	}
}

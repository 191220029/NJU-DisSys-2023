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

// Server Status
type Status int

const (
	LEADER = iota
	CANDIDATE
	FOLLOWER
)

// Timeouts (ms)
const TIMEOUT_MIN = 350
const TIMEOUT_RANGE = 150
const TIMEOUT_HEART_BEAT = 80

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

	status     Status
	timer      *time.Timer
	applyCh    chan ApplyMsg
	votes      int
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

	NextIndex int
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
//func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
//	// Your code here.
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//
//	// Receiver implementation:
//	//1. Reply false if term < currentTerm (§5.1)
//	//2. If votedFor is null or candidateId, and candidate’s log is at
//	//least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
//
//	if args.Term < rf.currentTerm {
//		reply.Term = rf.currentTerm
//		reply.VoteGranted = false
//	} else if args.Term > rf.currentTerm {
//		rf.currentTerm = args.Term
//		rf.votedFor = -1
//		rf.persist()
//		rf.beFollower()
//	} else {
//		reply.Term = rf.currentTerm
//
//		lastLogIndex := len(rf.logs) - 1
//		lastLogTerm := rf.logs[lastLogIndex].Term
//
//		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm) && args.LastLogIndex >= lastLogIndex) {
//			reply.VoteGranted = true
//			rf.votedFor = args.CandidateId
//			rf.persist()
//
//			if rf.status == FOLLOWER {
//				rf.resetTimer <- true
//			}
//		} else {
//			reply.VoteGranted = false
//		}
//	}
//}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm { // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		DPrintf("[RequestVote] server[%d] updates current term %d to server[%d](candidate)'s term %d.", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		rf.beFollower()
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[RequestVote] server[%d] rejects voting to server[%d](candidate). Candidate term < server[%d] currentTerm", rf.me, args.CandidateId, rf.me)
	} else {
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[lastLogIndex].Term
		// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && ((args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			DPrintf("[RequestVote] server[%d] votes to server[%d](candidate).", rf.me, args.CandidateId)
			if rf.status == FOLLOWER { // NOTICE: reset timer only for follower
				rf.resetTimer <- true // NOTICE: reset timer only when voting granted
			}
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			if !(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
				DPrintf("[RequestVote] server[%d] rejects voting to server[%d](candidate). votedFor is not null or candidateId", rf.me, args.CandidateId)
			}
			if !(args.LastLogTerm >= lastLogTerm && args.LastLogIndex >= lastLogIndex) {
				DPrintf("[RequestVote] server[%d] rejects voting to server[%d](candidate). Candidate's log is not as up-to-date as receiver's log.", rf.me, args.CandidateId)
			}
		}
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
//func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//
//	if ok && rf.status == CANDIDATE {
//		if rf.currentTerm < reply.Term {
//			rf.votedFor = -1
//			rf.currentTerm = reply.Term
//
//			rf.persist()
//			rf.beFollower()
//		} else if reply.VoteGranted == true && reply.Term == rf.currentTerm {
//			rf.votes += 1
//
//			if rf.votes > len(rf.peers)/2 {
//				rf.beLeader()
//			}
//		}
//	}
//
//	return ok
//}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.status == CANDIDATE {
		DPrintf("[sendRequestVote] server[%d](candidate) receives reply from server[%d]. Result is %t", rf.me, server, reply.VoteGranted)
		if rf.currentTerm < reply.Term { // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			rf.beFollower()
		} else {
			if reply.VoteGranted && reply.Term == rf.currentTerm { // NOTICE: 增加了应该是当前term的判断
				rf.votes++
				if rf.votes > len(rf.peers)/2 {
					DPrintf("[sendRequestVote] server[%d](candidate) receives votes from majority of servers.", rf.me)
					rf.beLeader()
				}
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
//func (rf *Raft) AppendEntries(arguments *AppendEntriesArguments, results *AppendEntriesResults) {
//	rf.mu.Lock()
//	defer rf.mu.Lock()
//
//	if arguments.Term < rf.current_term {
//		results.Term = rf.current_term
//		results.Success = false
//		return
//	}
//
//	if rf.status == FOLLOWER && arguments.Term > rf.current_term {
//		rf.current_term = arguments.Term
//		rf.votedFor = -1
//		rf.persist()
//	} else if rf.status == CANDIDATE {
//		rf.current_term = arguments.Term
//		rf.beFollower()
//	} else if arguments.Term > rf.current_term {
//		rf.current_term = arguments.Term
//		rf.beFollower()
//	}
//
//	if arguments.PrevLogIndex < 0 || arguments.PrevLogIndex >= len(rf.logs) || arguments.PrevLogTerm != rf.logs[arguments.PrevLogIndex].Term {
//		results.Term = rf.current_term
//		results.Success = false
//
//		if rf.status == FOLLOWER {
//			rf.resetTimer <- true
//		}
//
//		return
//	}
//
//	if arguments.PrevLogIndex+1 < len(rf.logs) {
//		rf.logs = rf.logs[:arguments.PrevLogIndex+1]
//	}
//
//	rf.logs = append(rf.logs, arguments.Entries...)
//
//	rf.persist()
//
//	if arguments.LeaderCommit > rf.commitIndex {
//		rf.commitIndex = min(arguments.LeaderCommit, len(rf.logs)-1)
//		rf.doApplyMsg()
//	}
//
//	results.Term = rf.current_term
//	results.Success = true
//
//	if rf.status == FOLLOWER {
//		rf.resetTimer <- true
//	}
//}

func (rf *Raft) AppendEntries(args *AppendEntriesArguments, reply *AppendEntriesResults) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 全部转成follower之后再做处理
	if rf.status == FOLLOWER {
		if args.Term > rf.currentTerm {
			DPrintf("[AppendEntries] server[%d](follower) updates current term %d to server[%d](leader)'s term %d.", rf.me, rf.currentTerm, args.LeaderId, args.Term)
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()
		}
	} else if rf.status == CANDIDATE {
		if args.Term >= rf.currentTerm {
			DPrintf("[AppendEntries] server[%d](candidate) updates current term %d to server[%d](leader)'s term %d.", rf.me, rf.currentTerm, args.LeaderId, args.Term)
			rf.currentTerm = args.Term
			rf.beFollower()
		}
	} else {
		if args.Term > rf.currentTerm {
			DPrintf("[AppendEntries] server[%d](old leader) updates current term %d to server[%d](leader)'s term %d.", rf.me, rf.currentTerm, args.LeaderId, args.Term)
			rf.currentTerm = args.Term
			rf.beFollower()
		}
	}

	if args.Term < rf.currentTerm { // Reply false if term < currentTerm
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = args.PrevLogIndex + 1
		DPrintf("[AppendEntries] server[%d] replies fail append entries to server[%d](leader). Leader's term < currentTerm", rf.me, args.LeaderId)
		return
	}

	if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Term = rf.currentTerm
		reply.Success = false
		if rf.status == FOLLOWER { // NOTICE: reset timer only for follower
			rf.resetTimer <- true // NOTICE
		}
		// lab3: optimize
		if args.PrevLogIndex >= len(rf.logs) {
			reply.NextIndex = len(rf.logs)
		} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			conflictTerm := rf.logs[args.PrevLogIndex].Term
			firstIndex := args.PrevLogIndex
			for firstIndex > 0 && rf.logs[firstIndex].Term == conflictTerm {
				firstIndex--
			}
			reply.NextIndex = firstIndex + 1
		}
		DPrintf("[AppendEntries] server[%d] replies fail append entries to server[%d](leader). Terms mismatch.", rf.me, args.LeaderId)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	if args.PrevLogIndex+1 < len(rf.logs) {
		DPrintf("[AppendEntries] server[%d]'s entry conflicts with a new one. previous logs: %v, current logs: %v", rf.me, rf.logs, rf.logs[:args.PrevLogIndex+1])
		rf.logs = rf.logs[:args.PrevLogIndex+1]
	}
	// Append any new entries not already in the log
	rf.logs = append(rf.logs, args.Entries...)
	rf.persist()
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.logs)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.logs) - 1
		}
		DPrintf("[AppendEntries] server[%d] set commitIndex to %d.", rf.me, rf.commitIndex)
		rf.doApplyMsg()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.NextIndex = len(rf.logs)
	if rf.status == FOLLOWER { // NOTICE: reset timer only for follower
		rf.resetTimer <- true
	}
	DPrintf("[AppendEntries] server[%d] replies success append entries to server[%d](leader). logs length: %d, logs: %v", rf.me, args.LeaderId, len(rf.logs), rf.logs)
}

//func (rf *Raft) sendAppendEntries(server int, arguments *AppendEntriesArguments, results *AppendEntriesResults) bool {
//	ok := rf.peers[server].Call("Raft.AppendEntries", arguments, results)
//
//	if ok == true {
//		rf.mu.Lock()
//		defer rf.mu.Unlock()
//
//		if rf.status == LEADER {
//			// If there exists an N such that N > commitIndex, a majority
//			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
//			// set commitIndex = N (§5.3, §5.4).
//			// If RPC request or response contains term T > currentTerm:
//			// set currentTerm = T, convert to follower (§5.1)
//			// If AppendEntries fails because of log inconsistency:
//			// decrement nextIndex and retry (§5.3)
//			if results.Success && results.Term == rf.current_term {
//				for n := len(rf.logs) - 1; n > rf.commitIndex; n-- {
//					if rf.current_term == rf.logs[n].Term {
//						count := 1
//						for i := 0; i < len(rf.peers); i++ {
//							if i != rf.me && rf.matchIndex[i] >= n {
//								count += 1
//							}
//						}
//						if count > len(rf.peers)/2 {
//							rf.commitIndex = n
//							break
//						}
//					}
//				}
//				rf.doApplyMsg()
//			} else if rf.current_term < results.Term {
//				rf.current_term = results.Term
//				rf.votedFor = -1
//				rf.persist()
//				rf.beFollower()
//			} else if rf.current_term >= results.Term {
//				rf.nextIndex[server] -= 1
//				rf.sendAppendEntries(server, arguments, results)
//			}
//		}
//	}
//
//	return ok
//}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArguments, reply *AppendEntriesResults) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		DPrintf("[sendAppendEntries] server[%d](leader) receives reply from server[%d]. Result is %t", rf.me, server, reply.Success)
		if rf.status == LEADER {
			if reply.Success && reply.Term == rf.currentTerm { // NOTICE: 增加了应该是当前term的判断
				rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[server] = rf.nextIndex[server] - 1

				// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and logs[N].term == currentTerm: set commitIndex = N
				for N := len(rf.logs) - 1; N > rf.commitIndex; N-- {
					if rf.logs[N].Term == rf.currentTerm { // Figure 8
						count := 1
						for i := 0; i < len(rf.peers); i++ {
							if i != rf.me && rf.matchIndex[i] >= N {
								count++
							}
						}
						if count > len(rf.peers)/2 {
							DPrintf("[sendAppendEntries] server[%d](leader) sets commitIndex from %d to %d.", rf.me, rf.commitIndex, N)
							rf.commitIndex = N
							break
						}
					}
				}
				rf.doApplyMsg()
			} else {
				if rf.currentTerm < reply.Term { // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
					DPrintf("[sendAppendEntries] server[%d](old leader) updates current term %d to reply term %d from server[%d].", rf.me, rf.currentTerm, reply.Term, server)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
					rf.beFollower()
				} else {
					// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
					rf.nextIndex[server] = reply.NextIndex
				}
			}
		}
		rf.mu.Unlock()
	} else {
		DPrintf("[sendAppendEntries] server[%d](leader) cannot receive from server[%d].", rf.me, server)
	}
	return ok
}

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
//func (rf *Raft) Start(command interface{}) (int, int, bool) {
//	index := -1
//	term := -1
//	isLeader := true
//
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//
//	term = rf.currentTerm
//	isLeader = rf.status == LEADER
//
//	if isLeader == true {
//		rf.logs = append(
//			rf.logs,
//			LogEntry{
//				Term:    term,
//				Command: command,
//			},
//		)
//
//		rf.persist()
//
//		index = len(rf.logs) - 1
//	}
//
//	return index, term, isLeader
//}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.status == LEADER
	term = rf.currentTerm

	if isLeader {
		rf.logs = append(rf.logs, LogEntry{
			Term:    term,
			Command: command,
		})
		rf.persist()
		rf.nextIndex[rf.me] = len(rf.logs)
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
		index = len(rf.logs) - 1
		DPrintf("[Start] start an agreement. current leader: server[%d], logs length: %d, logs: %v, index = %d, term = %d", rf.me, len(rf.logs), rf.logs, index, term)
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
//func Make(peers []*labrpc.ClientEnd, me int,
//	persister *Persister, applyCh chan ApplyMsg) *Raft {
//	rf := &Raft{}
//	rf.peers = peers
//	rf.persister = persister
//	rf.me = me
//
//	// Your initialization code here.
//	rf.currentTerm = 0
//	rf.votedFor = -1
//	rf.logs = append(rf.logs, LogEntry{-1, nil})
//	rf.commitIndex = 0
//	rf.lastApplied = 0
//
//	rf.status = FOLLOWER
//	rf.timer = time.NewTimer(time.Duration(TIMEOUT_MIN+rand.Intn(TIMEOUT_RANGE)) * time.Millisecond)
//	rf.applyCh = applyCh
//
//	// initialize from state persisted before a crash
//	rf.readPersist(persister.ReadRaftState())
//
//	// start concurrent state machine
//	go rf.run()
//
//	return rf
//}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	DPrintf("[Make] create Raft server[%d].", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.status = FOLLOWER
	rf.timer = time.NewTimer(time.Duration(TIMEOUT_MIN+rand.Intn(TIMEOUT_RANGE)) * time.Millisecond)
	rf.resetTimer = make(chan bool, 5)
	rf.commitIndex = 0
	rf.votes = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.logs = append(rf.logs, LogEntry{-1, nil}) // NOTICE: insert an empty log!

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.loopAsFollower()
	return rf
}

//func (rf *Raft) beFollower() {
//	if rf.status != FOLLOWER {
//		rf.status = FOLLOWER
//		rf.votedFor = -1
//		rf.votes = 0
//		rf.resetTimer = make(chan bool, 5)
//		rf.persist()
//		go rf.run()
//	}
//}

func (rf *Raft) beFollower() {
	if rf.status != FOLLOWER {
		DPrintf("[convert2Follower] server[%d] converts to follower.", rf.me)
		rf.status = FOLLOWER
		rf.timer = time.NewTimer(time.Duration(TIMEOUT_MIN+rand.Intn(TIMEOUT_RANGE)) * time.Millisecond)
		rf.votes = 0
		rf.votedFor = -1
		rf.resetTimer = make(chan bool, 5)
		rf.persist()
		go rf.loopAsFollower()
	}
}

func (rf *Raft) beCandidate() {
	rf.status = CANDIDATE
	rf.resetTimer = make(chan bool, 5)
}

//func (rf *Raft) beLeader() {
//	if rf.status != LEADER {
//		rf.status = LEADER
//
//		rf.nextIndex = make([]int, len(rf.peers))
//		rf.matchIndex = make([]int, len(rf.peers))
//
//		for i := 0; i < len(rf.peers); i++ {
//			rf.nextIndex[i] = len(rf.logs)
//			rf.matchIndex[i] = 0
//		}
//	}
//
//	go rf.run()
//}

func (rf *Raft) beLeader() {
	if rf.status != LEADER {
		DPrintf("[convert2Leader] server[%d] converts to leader.", rf.me)
		rf.status = LEADER
		// Volatile state on leaders. Reinitialized after election.
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.logs)
			rf.matchIndex[i] = 0
		}
		go rf.loopAsLeader()
	}
}

func (rf *Raft) run() {
	rf.mu.Lock()

	switch rf.status {
	//case LEADER:
	//	{
	//		// Upon election: send initial empty AppendEntries RPCs
	//		// (heartbeat) to each server; repeat during idle periods to
	//		// prevent election timeouts (§5.2)
	//		for i := range rf.peers {
	//			if i != rf.me {
	//				entriesArgs := &AppendEntriesArguments{
	//					Term:         rf.currentTerm,
	//					LeaderId:     rf.me,
	//					PrevLogIndex: rf.nextIndex[i] - 1,
	//					PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
	//					Entries:      make([]LogEntry, 0),
	//					LeaderCommit: rf.commitIndex,
	//				}
	//
	//				entriesResult := &AppendEntriesResults{}
	//
	//				go rf.sendAppendEntries(i, entriesArgs, entriesResult)
	//			}
	//		}
	//
	//		rf.timer = time.NewTimer(time.Duration(TIMEOUT_HEART_BEAT) * time.Millisecond)
	//
	//		rf.mu.Unlock()
	//		if rf.status == LEADER {
	//			select {
	//			case <-rf.timer.C:
	//				{
	//				}
	//			}
	//		}
	//
	//		for rf.status == LEADER {
	//			rf.mu.Lock()
	//
	//			for i := range rf.peers {
	//				if i != rf.me {
	//					go rf.sendAppendEntries(
	//						i,
	//						&AppendEntriesArguments{
	//							Term:         rf.currentTerm,
	//							LeaderId:     rf.me,
	//							LeaderCommit: rf.commitIndex,
	//							Entries:      rf.logs[rf.nextIndex[i]:],
	//							PrevLogIndex: rf.nextIndex[i] - 1,
	//							PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
	//						},
	//						&AppendEntriesResults{},
	//					)
	//				}
	//			}
	//
	//			if rf.status == LEADER {
	//				rf.timer = time.NewTimer(time.Duration(TIMEOUT_HEART_BEAT) * time.Millisecond)
	//			}
	//
	//			rf.mu.Unlock()
	//		}
	//
	//		rf.mu.Unlock()
	//		if rf.status == LEADER {
	//			select {
	//			case <-rf.timer.C:
	//				{
	//				}
	//			}
	//		}
	//		return
	//	}
	//case FOLLOWER:
	//	{
	//		rf.mu.Unlock()
	//		for rf.status == FOLLOWER {
	//			select {
	//			case reset := <-rf.resetTimer:
	//				if reset {
	//					rf.timer = time.NewTimer(time.Duration(TIMEOUT_MIN+rand.Intn(TIMEOUT_RANGE)) * time.Millisecond)
	//				}
	//			case <-rf.timer.C:
	//				// If election timeout elapses without receiving AppendEntries
	//				// RPC from current leader or granting vote to candidate:
	//				// convert to candidate
	//				rf.mu.Lock()
	//				rf.beCandidate()
	//				rf.mu.Unlock()
	//				go rf.run()
	//			}
	//		}
	//	}
	//case CANDIDATE:
	//	rf.mu.Unlock()
	//	for rf.status == CANDIDATE {
	//		rf.mu.Lock()
	//		if rf.status != CANDIDATE {
	//			rf.mu.Unlock()
	//			return
	//		}
	//
	//		// On conversion to candidate, start election:
	//		// Increment currentTerm
	//		// Vote for self
	//		// Reset election timer
	//		// Send RequestVote RPCs to all other servers
	//		rf.currentTerm += 1
	//		rf.votedFor = rf.me
	//		rf.votes = 1
	//		rf.persist()
	//
	//		rf.timer = time.NewTimer(time.Duration(TIMEOUT_MIN+rand.Intn(TIMEOUT_RANGE)) * time.Millisecond)
	//		for i := range rf.peers {
	//			if i != rf.me {
	//				go rf.sendRequestVote(
	//					i,
	//					RequestVoteArgs{
	//						Term:         rf.currentTerm,
	//						CandidateId:  rf.me,
	//						LastLogIndex: len(rf.logs) - 1,
	//						LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	//					},
	//					&RequestVoteReply{},
	//				)
	//			}
	//		}
	//		rf.mu.Unlock()
	//
	//		if rf.status == CANDIDATE {
	//			select {
	//			case <-rf.timer.C:
	//			}
	//		}
	//	}

	case LEADER:
		rf.mu.Unlock()
		rf.loopAsLeader()
	case FOLLOWER:
		rf.mu.Unlock()
		rf.loopAsFollower()
	case CANDIDATE:
		rf.mu.Unlock()
		rf.loopAsCandidate()
	}
}

func (rf *Raft) loopAsFollower() {
	for rf.status == FOLLOWER {
		DPrintf("follower loop: %d, currentTerm: %d, commitIndex: %d\n", rf.me, rf.currentTerm, rf.commitIndex)
		select {
		case reset := <-rf.resetTimer:
			if reset {
				DPrintf("[loopAsFollower] server[%d] resets timer.", rf.me)
				rf.timer = time.NewTimer(time.Duration(TIMEOUT_MIN+rand.Intn(TIMEOUT_RANGE)) * time.Millisecond)
			}
		case <-rf.timer.C: // election timeout, become candidate
			rf.mu.Lock()
			rf.status = CANDIDATE
			rf.resetTimer = make(chan bool, 5)
			rf.mu.Unlock()
			go rf.loopAsCandidate()
		}
	}
}

func (rf *Raft) loopAsCandidate() {
	for rf.status == CANDIDATE {
		//fmt.Printf("candidate: %d, commitIndex: %d\n", rf.me, rf.commitIndex)
		rf.mu.Lock()
		if rf.status != CANDIDATE {
			rf.mu.Unlock()
			return
		}
		rf.currentTerm++    // Increment currentTerm
		rf.votedFor = rf.me // Vote for self
		rf.votes = 1
		rf.persist()
		DPrintf("[loopAsCandidate] server[%d] becomes candidate. Current term: %d", rf.me, rf.currentTerm)
		rf.timer = time.NewTimer(time.Duration(TIMEOUT_MIN+rand.Intn(TIMEOUT_RANGE)) * time.Millisecond) // Reset election timer
		// Send RequestVote RPCs to all other servers
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := &RequestVoteArgs{}
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				args.LastLogIndex = len(rf.logs) - 1
				args.LastLogTerm = rf.logs[args.LastLogIndex].Term
				reply := &RequestVoteReply{}
				DPrintf("[loopAsCandidate] server[%d](candidate) sends request vote to server[%d].", rf.me, i)
				go rf.sendRequestVote(i, args, reply)
			}
		}
		rf.mu.Unlock()
		if rf.status == CANDIDATE {
			select {
			case <-rf.timer.C: // election timeout, start new election
				if rf.status == CANDIDATE {
					DPrintf("[loopAsCandidate] election timeout. Server[%d](candidate) starts new election.", rf.me)
				}
			}
		}
	}
}

func (rf *Raft) loopAsLeader() {
	rf.mu.Lock()
	if rf.status == LEADER {
		// Notice: Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := &AppendEntriesArguments{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				args.Entries = make([]LogEntry, 0)
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				reply := &AppendEntriesResults{}
				DPrintf("[loopAsLeader] server[%d](leader) sends initial empty AppendEntries RPCs (heartbeat) to server[%d]. Leader term: %d. args: %v", rf.me, i, rf.currentTerm, *args)
				go rf.sendAppendEntries(i, args, reply)
			}
		}
		rf.timer = time.NewTimer(time.Duration(TIMEOUT_HEART_BEAT) * time.Millisecond)
		DPrintf("[loopAsLeader] server[%d](leader) resets timer.", rf.me)
	}
	rf.mu.Unlock()
	if rf.status == LEADER {
		select {
		case <-rf.timer.C: // heartbeat timeout
			if rf.status == LEADER {
				DPrintf("[loopAsLeader] server[%d](leader) heartbeat timeout.", rf.me)
			}
		}
	}

	for rf.status == LEADER {
		//fmt.Printf("leader: %d, commitIndex: %d, logs length: %d\nnextIndex: %v, matchIndex: %v\n", rf.me, rf.commitIndex, len(rf.logs), rf.nextIndex, rf.matchIndex)
		rf.mu.Lock()
		if rf.status != LEADER {
			rf.mu.Unlock()
			return
		}
		// Send AppendEntries RPCs to all other server
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := &AppendEntriesArguments{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				args.Entries = rf.logs[rf.nextIndex[i]:]
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				reply := &AppendEntriesResults{}
				DPrintf("[loopAsLeader] server[%d](leader) sends append entries to server[%d]. Leader term: %d. args: %v", rf.me, i, rf.currentTerm, *args)
				go rf.sendAppendEntries(i, args, reply)
			}
		}

		if rf.status == LEADER {
			rf.timer = time.NewTimer(time.Duration(TIMEOUT_HEART_BEAT) * time.Millisecond)
			DPrintf("[loopAsLeader] server[%d](leader) resets timer.", rf.me)
		}
		rf.mu.Unlock()
		if rf.status == LEADER {
			select {
			case <-rf.timer.C: // heartbeat timeout, send append entries
				if rf.status == LEADER {
					DPrintf("[loopAsLeader] server[%d](leader) heartbeat timeout.", rf.me)
				}
			}
		}
	}
}

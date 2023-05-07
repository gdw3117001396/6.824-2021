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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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
type RaftState int

const (
	Follower  RaftState = 0
	Candidate RaftState = 1
	Leader    RaftState = 2
)

const (
	heartsbeatsMs     = 100
	electionTimeMinMs = 250
	electionTimeMaxMs = 400
)

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
	currentTerm int // 此节点的任期
	votedFor    int // 此节点投票给了谁

	//pivate:
	electionTime time.Time
	state        RaftState
}

type Log struct {
	index   int         // 该记录在日志中的位置
	term    int         // 该记录首次被创建时的任期号
	command interface{} // 命令
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // Candidate的任期
	CandidateId int // candidate请求投票
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 此节点的任期。假如 Candidate 发现 Follower 的任期高于自己，则会放弃 Candidate 身份并更新自己的任期。
	VoteGranted bool // 是否同意candidated当选，true表示同意
}

//
// example RequestVote RPC handler.
// 请求投票RPC
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 如果term < currentTerm,拒绝投票，否则如果rf.votedFor == null or candidateId同意
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//  Reply false if term < currentTerm 如果args的term小于rf当前任期，拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// term T > currentTerm:
	//      1.set currentTerm = T,
	// 		2. convert to follower
	//      3. 论文隐含了一定要重置选票，不然可能会导致后面投不了票的
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	// 到这里一定有rf.currentTerm == args.term了, 所以必然会有args.term == reply.term了，后面double check才要检查
	reply.Term = rf.currentTerm
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		DPrintf("%d 在 term %d 给 %d 投票", rf.me, rf.currentTerm, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// 给其它节点投票了也要重置超时时间
		rf.resetElectionTime()
	}
}

// 在领导选举的过程中，AppendEntries RPC 用来实现 Leader 的心跳机制。节点的 AppendEntries RPC 会被 Leader 定期调用。
type AppendEntriesArgs struct {
	Term     int // Leader 的任期
	LeaderId int // Client 可能将请求发送至 Follower 节点，得知 leaderId 后 Follower 可将 Client 的请求重定位至 Leader 节点
}

type AppendEntriesReply struct {
	Term    int  //此节点的任期,假如 Leader 发现 Follower 的任期高于自己，则会放弃 Leader 身份并更新自己的任期。
	Success bool // 此节点是否认同 Leader 发送的心跳。
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//  Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	//     term T > currentTerm:
	//      1.set currentTerm = T,
	// 		2. convert to follower
	//      3. 论文隐含了一定要重置选票
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	// 收到心跳重置超时时间
	rf.resetElectionTime()
	reply.Term = rf.currentTerm
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

func (rf *Raft) loadState() RaftState {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) loadElectionTime() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.electionTime
}

// 三个转换形态函数使用前都要加锁
// 成为跟随者
func (rf *Raft) toFollower() {
	DPrintf("%d 在term %d 变成了Follower", rf.me, rf.currentTerm)
	rf.state = Follower
}

// 成为候选人，任期+1，投自己一票，重置选举时间
func (rf *Raft) toCandidate() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.resetElectionTime()
	DPrintf("%d 在term %d 变成了Candidate并发起选举", rf.me, rf.currentTerm)
}

// 成为leader，开启心跳
func (rf *Raft) toLeader() {
	DPrintf("%d 在term %d 变成了Leader", rf.me, rf.currentTerm)
	rf.state = Leader
	// 开启心跳
	go rf.heartsbeat()
}

func (rf *Raft) election() {
	rf.mu.Lock()
	// 成为Ccandidate
	rf.toCandidate()
	// 请求选票的RequestVoteArgs都要相同
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	voteCount := 1
	rf.mu.Unlock()
	// DPrintf("%d 在term %d 开始发起选举", rf.me, rf.currentTerm)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.requestVotetoAllPeer(i, args, &voteCount)
	}
}

func (rf *Raft) requestVotetoAllPeer(serverId int, args *RequestVoteArgs, voteCount *int) {
	replys := &RequestVoteReply{}
	rf.mu.Lock()
	// 每次请求投票时一定要看看自己还是不是Candidate
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	if ok := rf.sendRequestVote(serverId, args, replys); !ok {
		// 这里不加锁会data race
		// DPrintf("%d 在term %d 发送给 %d 请求投票RPC失败", rf.me, rf.currentTerm, i)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//  term T > currentTerm: set currentTerm = T, convert to follower
	if replys.Term > rf.currentTerm {
		rf.currentTerm = replys.Term
		rf.votedFor = -1 // 一定要重置选票
		rf.toFollower()
		return
	}
	// double check，因为有可能发起新一轮选举了，才收到这个消息
	// 当前角色是否还需要这个动作，当前term是否还需要处理这个回复, 回复的 term 是否与发起的 term 一致
	if rf.state != Candidate || rf.currentTerm != replys.Term || replys.Term != args.Term {
		return
	}
	if replys.VoteGranted {
		*voteCount++
		DPrintf("%d 在term %d 收到了 %d 的投票, 现在总共票数 %d", rf.me, rf.currentTerm, serverId, voteCount)
		if *voteCount > len(rf.peers)/2 {
			rf.toLeader()
		}
	}

}
func (rf *Raft) heartsbeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		DPrintf("%d 在term %d 开始发送心跳", rf.me, rf.currentTerm)

		rf.mu.Unlock()
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.sendAppendEntriesToPeer(i)
		}
		time.Sleep(heartsbeatsMs * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntriesToPeer(serverid int) {
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	reply := &AppendEntriesReply{}
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	if ok := rf.sendAppendEntries(serverid, args, reply); !ok {
		// DPrintf("%d 在term %d 发送给 %d 心跳RPC失败", rf.me, rf.currentTerm, i)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d 在 term %d 收到 %d 的AppendEntries reply %+v", rf.me, rf.currentTerm, serverid, reply)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1 // 重置选票
		rf.toFollower()
		return
	}
	if rf.state != Leader || rf.currentTerm != reply.Term || args.Term != reply.Term {
		return
	}
}

// 需要在加锁状态下使用
func (rf *Raft) resetElectionTime() {
	electionTime := time.Duration((electionTimeMinMs + rand.Intn(electionTimeMaxMs-electionTimeMinMs))) * time.Millisecond
	time := time.Now()
	rf.electionTime = time.Add(electionTime)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.state != Leader && time.Now().After(rf.electionTime) {
			// 开启选举
			rf.mu.Unlock()
			rf.election()
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(5 * time.Millisecond)
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.toFollower()
	rf.resetElectionTime()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

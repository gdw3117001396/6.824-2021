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
	log         Log // 日志条目， first index is 1, 每条 Entry 包含一条待施加至状态机的命令

	// Volatile state on all servers:
	commitIndex int // 已经提交了的最高日志id, 初始化为0，自动增长(当Leader成功在大部分server上复制了一条Entry那么这条日志就是已提交)
	lastApplied int // 已经应用给上层状态机的最高日志id, 初始化为0，自动增长

	// Volatile state on learders:意味着只能leader有且leader才有意义，也就是在成为leader时要重新初始化
	// 在一次次心跳中，nextIndex 不断减小，matchIndex 不断增大，直至 matchIndex = nextIndex - 1，则代表该 Follower 已经与 Leader 成功同步。
	nextIndex  []int // 需要同步给peer[i]的下一个日志条目entry的index，初始化为last log index + 1
	matchIndex []int // 代表Leader已知的已在peer[i]上成功复制的最高entry index, 初始化为0（帮助Leader更新commitIndex）

	//Snapshot
	snapshot      []byte // 保存的最近一个状态机快照
	snapshotIndex int    // 快照包含的最后一个entry的index
	snapshotTerm  int    //  lastIncludedIndex对应的term

	//相当于一个中间状态，用来apply给上层状态机的
	waitingSnapshot []byte
	waitingIndex    int
	waitingTerm     int
	//pivate:
	electionTime time.Time
	state        RaftState
	applyCh      chan ApplyMsg
	applyCond    *sync.Cond
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotargs, reply *InstallSnapshotreplys) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
// 第一个返回值是index, 第二个是current term, 第三个是否是Leader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	index := rf.log.lastindex() + 1
	term := rf.currentTerm
	rf.log.append(Entry{Term: term, Command: command})
	rf.persist()
	DPrintf("%d 在 term %d 写入了日志, LogIndex : %d , LogTerm : %d, Command: %v", rf.me, rf.currentTerm, index, term, command)
	rf.sendAppendEntriesToAllPeerL(false)
	return index, term, true
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

// 成为跟随者, 重置投票，设置currentTerm
func (rf *Raft) toFollowerL(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	DPrintf("%d 在term %d 变成了Follower", rf.me, rf.currentTerm)
}

// 成为候选人，任期+1，投自己一票
func (rf *Raft) toCandidateL() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.persist()
	DPrintf("%d 在term %d 变成了Candidate并发起选举", rf.me, rf.currentTerm)
}

// 成为leader，开启心跳
func (rf *Raft) toLeaderL() {
	DPrintf("%d 在term %d 变成了Leader", rf.me, rf.currentTerm)
	rf.state = Leader
	n := len(rf.peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	lastLogIndex := rf.log.lastindex()
	for i := 0; i < n; i++ {
		rf.nextIndex[i] = lastLogIndex + 1
	}
}

func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
}
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastApplied = 0
	if rf.lastApplied+1 <= rf.log.start() {
		rf.lastApplied = rf.log.start()
	}
	for !rf.killed() {
		if rf.waitingSnapshot != nil {
			DPrintf("%d 在term %d deliver snapshot", rf.me, rf.currentTerm)
			applyMsg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.waitingSnapshot,
				SnapshotIndex: rf.waitingIndex,
				SnapshotTerm:  rf.waitingTerm,
			}
			rf.waitingSnapshot = nil
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else if rf.lastApplied+1 <= rf.commitIndex &&
			rf.lastApplied+1 <= rf.log.lastindex() &&
			rf.lastApplied+1 > rf.log.start() {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.log.entry(rf.lastApplied).Command,
			}
			rf.mu.Unlock()
			// 不要在锁的状态下用channel
			rf.applyCh <- applyMsg
			rf.mu.Lock()
			DPrintf("%d 在 term %d 往状态机成功发送 Msg %+v", rf.me, rf.currentTerm, applyMsg)
		} else {
			rf.applyCond.Wait()
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
	DPrintf("服务器开始运行")
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.log = mkLogEmpty()
	// rf.log.append()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.resetElectionTimeL()

	// start ticker goroutine to start elections
	go rf.ticker()
	// 提交日志给State Machine
	go rf.applier()
	return rf
}

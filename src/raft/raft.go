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
	heartsbeatsMs = 100
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
	//pivate:
	electionTime time.Time
	state        RaftState
	applyCh      chan ApplyMsg
	applyCond    *sync.Cond
}

/*
type LogEntry struct {
	Index   int         // 该记录在日志中的位置
	Term    int         // 该记录首次被创建时的任期号
	Command interface{} // 命令
}*/

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
	Term         int // Candidate的任期
	CandidateId  int // Candidate请求投票
	LastLogIndex int // Candidate的最后log的index
	LastLogTerm  int // Candidate的最后log的term
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
	// All Servers Rule 2
	if args.Term > rf.currentTerm {
		rf.toFollowerL(args.Term)
	}
	// 到这里一定有rf.currentTerm == args.term了, 所以必然会有args.term == reply.term了，后面double check才要检查
	reply.Term = rf.currentTerm
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := rf.log.lastindex()
		lastLogTerm := rf.log.entry(lastLogIndex).Term
		if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
			reply.VoteGranted = false
			return
		}
		DPrintf("%d 在 term %d 给 %d 投票, lastLogIndex : %d , lastLogTerm(-1表示日志还是空的) : % d", rf.me, rf.currentTerm, args.CandidateId, lastLogIndex, lastLogTerm)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// 给其它节点投票了才要重置超时时间，没投票不能重置超时时间
		rf.resetElectionTimeL()
	}
}

// 心跳和同步日志
type AppendEntriesArgs struct {
	Term         int     // Leader 的任期
	LeaderId     int     // Client 可能将请求发送至 Follower 节点，得知 leaderId 后 Follower 可将 Client 的请求重定位至 Leader 节点
	PrevLogIndex int     // 添加日志Entries的前一条Entry的index
	PrevLogTerm  int     // prevLogIndex对应entry的term
	Entries      []Entry //需要同步的entries。若为空，则是heartbeat
	LeaderCommit int     // Leader的commitIndex，帮助Follower更新自身的commitIndex
}

type AppendEntriesReply struct {
	Term     int  //此节点的任期,假如 Leader 发现 Follower 的任期高于自己，则会放弃 Leader 身份并更新自己的任期。
	Success  bool // 此节点是否认同 Leader 发送的心跳。
	Conflict bool
	XTerm    int // 冲突 entry 的任期
	XIndex   int // XTerm 的第一条 entry 的 index
	XLen     int // 自己log本身的长度
}

// 心跳和日志条目
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//  Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollowerL(args.Term)
	}

	// 收到心跳重置超时时间
	rf.resetElectionTimeL()
	// 因为前面可能会修改了rf.currentTerm所以要赋值，一定要确保reply.Term是最新的currentTerm
	reply.Term = rf.currentTerm

	// 2.日志不匹配需要leader的日志回退(5.3) (如果自己的log数量比prevLogIndex少直接就是不匹配了可以返回false)
	if rf.log.lastindex() < args.PrevLogIndex {
		DPrintf("%d 在 term %d 收到来自 %d 的日志不匹配1, rf.log: %+v", rf.me, rf.currentTerm, args.LeaderId, rf.log)
		reply.Success = false
		reply.Conflict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.log.len()
		return
	}
	if rf.log.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		DPrintf("%d 在 term %d 收到来自 %d 的日志不匹配2, rf.log: %+v", rf.me, rf.currentTerm, args.LeaderId, rf.log)
		reply.Success = false
		reply.Conflict = true
		xTerm := rf.log.entry(args.PrevLogIndex).Term
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.log.entry(xIndex-1).Term != xTerm {
				reply.XIndex = xIndex
				break
			}
		}
		reply.XTerm = xTerm
		reply.XLen = rf.log.len()
		return
	}
	reply.Success = true
	DPrintf("%d 在 term %d 收到来自 %d 的AppendEntries, PrevLogIndex: %d , PrevLogTerm: %d ", rf.me, rf.currentTerm, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)

	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		// 3.日志冲突, 删除
		if index <= rf.log.lastindex() && rf.log.entry(index).Term != entry.Term {
			rf.log.cutend(index)
			// DPrintf("%d 在 term %d 收到了日志冲突, 删除, 当前日志为: %+v", rf.me, rf.currentTerm, rf.log)
		}
		// 4.添加不存在的新的日志,Append any new entries not already in the log
		if index > rf.log.lastindex() {
			rf.log.append(args.Entries[i:]...)
			// DPrintf("%d 在 term %d 收到日志追加, %+v .", rf.me, rf.currentTerm, rf.log)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastindex())
		rf.apply()
	}
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
	DPrintf("%d 在 term %d 写入了日志, LogIndex : %d , LogTerm : %d, Command: %v", rf.me, rf.currentTerm, index, term, command)
	rf.sendAppendEntriesToAllPeerL()
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

// 日志工具类
/*func (rf *Raft) getLastLogIndex() int {
	return len(rf.log)
}

// getLog
func (rf *Raft) getLog(index int) LogEntry {
	if index == 0 {
		return LogEntry{Term: -1, Index: -1}
	} else {
		return rf.log[index-1]
	}
}

// subLog 包括: start 和 end, 深拷贝，不然极限情况下会有 race 的 bug
func (rf *Raft) subLog(start int, end int) []LogEntry {
	// 从头拷贝到end
	if start == -1 {
		return append([]LogEntry{}, rf.log[:end]...)
	} else if end == -1 { // 从start拷贝到尾
		return append([]LogEntry{}, rf.log[start-1:]...)
	} else {
		return append([]LogEntry{}, rf.log[start-1:end]...)
	}
}*/
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 成为跟随者, 重置投票，设置currentTerm
func (rf *Raft) toFollowerL(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	DPrintf("%d 在term %d 变成了Follower", rf.me, rf.currentTerm)
}

// 成为候选人，任期+1，投自己一票
func (rf *Raft) toCandidateL() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
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
	// 发送心跳
	rf.sendAppendEntriesToAllPeerL()

	// 开启提交日志
	go rf.leaderCommit()
}

func (rf *Raft) leaderCommit() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		for n := rf.commitIndex + 1; n <= rf.log.lastindex(); n++ {
			if rf.log.entry(n).Term != rf.currentTerm {
				continue
			}
			matchIndexCount := 1
			for serverId := 0; serverId < len(rf.peers); serverId++ {
				if serverId != rf.me && rf.matchIndex[serverId] >= n {
					matchIndexCount++
					if matchIndexCount > len(rf.peers)/2 {
						rf.commitIndex = n
						rf.apply()
						break
					}
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) electionL() {
	// 成为Ccandidate
	rf.toCandidateL()
	// 请求选票的RequestVoteArgs都要相同
	lastLogIndex := rf.log.lastindex()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log.entry(lastLogIndex).Term,
	}
	voteCount := 1
	// DPrintf("%d 在term %d 开始发起选举", rf.me, rf.currentTerm)
	for i := range rf.peers {
		if i != rf.me {
			go rf.requestVote(i, args, &voteCount)
		}
	}
}

func (rf *Raft) requestVote(serverId int, args *RequestVoteArgs, voteCount *int) {
	replys := &RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, replys)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//  term T > currentTerm: set currentTerm = T, convert to follower
		if replys.Term > rf.currentTerm {
			rf.toFollowerL(replys.Term)
			return
		}
		// double check，因为有可能发起新一轮选举了，才收到这个消息
		// 是否还在同一个时期
		if rf.state != Candidate || replys.Term != args.Term {
			return
		}
		if replys.VoteGranted {
			*voteCount++
			DPrintf("%d 在term %d 收到了 %d 的投票, 现在总共票数 %d", rf.me, rf.currentTerm, serverId, *voteCount)
			if *voteCount > len(rf.peers)/2 {
				rf.toLeaderL()
				rf.sendAppendEntriesToAllPeerL()
			}
		}
	}
}

func (rf *Raft) sendAppendEntriesToAllPeerL() {
	DPrintf("%d 在term %d 开始发送AppendEntries, nextIndex: %+v", rf.me, rf.currentTerm, rf.nextIndex)
	for i := range rf.peers {
		if i != rf.me {
			rf.sendAppendEntriesToPeerL(i)
		}
	}
}
func (rf *Raft) sendAppendEntriesToPeerL(serverid int) {
	next := rf.nextIndex[serverid]
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: next - 1,
		PrevLogTerm:  rf.log.entry(next - 1).Term,
		Entries:      make([]Entry, rf.log.lastindex()-next+1),
		LeaderCommit: rf.commitIndex,
	}
	copy(args.Entries, rf.log.slice(next))
	DPrintf("%d 在term %d 给 %d 发送AppendEntries, rf.nextIndex[%d]: %d, args.PrevLogIndex %d, args.PrevLogTerm %d", rf.me, rf.currentTerm, serverid, serverid, rf.nextIndex[serverid], args.PrevLogIndex, args.PrevLogTerm)
	go func() {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(serverid, args, reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != Leader {
				return
			}
			rf.processAppendReplyL(serverid, args, reply)
		}
	}()
}
func (rf *Raft) processAppendReplyL(serverid int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%d 在 term %d 收到 %d 的AppendEntries reply %+v", rf.me, rf.currentTerm, serverid, reply)
	if reply.Term > rf.currentTerm {
		rf.toFollowerL(reply.Term)
		return
	}
	// 看看是否还是同一个时期
	if args.Term != reply.Term {
		return
	}
	if reply.Success {
		newNext := args.PrevLogIndex + len(args.Entries) + 1
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newNext > rf.nextIndex[serverid] {
			rf.nextIndex[serverid] = newNext
		}
		if newMatch > rf.matchIndex[serverid] {
			rf.matchIndex[serverid] = newMatch
		}
		DPrintf("%d 在 term %d 更新了nextInde[%d]: %d , 更新了matchIndex[%d]: %d .", rf.me, rf.currentTerm, serverid, rf.nextIndex[serverid], serverid, rf.matchIndex[serverid])
	} else if reply.Conflict {
		if reply.XTerm == -1 {
			rf.nextIndex[serverid] = reply.XLen
			DPrintf("%d 在 term %d 拒绝了 %d 的日志传送，日志条目太少, nextIndex[%d]: %d .", serverid, rf.currentTerm, rf.me, serverid, rf.nextIndex[serverid])
		} else {
			lastLogInXTermIndex := rf.findLastLogInTerm(reply.XTerm)
			if lastLogInXTermIndex == -1 {
				rf.nextIndex[serverid] = reply.XIndex
				DPrintf("%d 在 term %d 拒绝了 %d 的日志传送, 没有Xterm, nextIndex[%d]: %d .", serverid, rf.currentTerm, rf.me, serverid, rf.nextIndex[serverid])
			} else {
				rf.nextIndex[serverid] = lastLogInXTermIndex
				DPrintf("%d 在 term %d 拒绝了 %d 的日志传送,有Xterm,nextIndex[%d]: %d .", serverid, rf.currentTerm, rf.me, serverid, rf.nextIndex[serverid])
			}
		}
	} else {
		rf.nextIndex[serverid] = args.PrevLogIndex
		DPrintf("%d 在 term %d 拒绝了 %d 的日志传送, nextIndex[%d]: %d .", serverid, rf.currentTerm, rf.me, serverid, rf.nextIndex[serverid])
	}
}
func (rf *Raft) findLastLogInTerm(x int) int {
	for i := rf.log.lastindex(); i > 0; i-- {
		term := rf.log.entry(i).Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}
func (rf *Raft) resetElectionTimeL() {
	t := time.Now()
	electionTimeout := time.Duration((150 + rand.Intn(150))) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.tick()
		time.Sleep(heartsbeatsMs * time.Millisecond)
	}
}

func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		rf.resetElectionTimeL()
		// 发送AppendEntries
		rf.sendAppendEntriesToAllPeerL()
	}
	if time.Now().After(rf.electionTime) {
		rf.resetElectionTimeL()
		rf.electionL()
	}
}
func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
}
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.killed() == false {
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				// Command:      rf.getLog(rf.lastApplied).Command,
				Command: rf.log.entry(rf.lastApplied).Command,
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
	rf.resetElectionTimeL()
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.log = mkLogEmpty()
	rf.log.append(Entry{0, -1})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// 提交日志给State Machine
	go rf.applier()
	return rf
}

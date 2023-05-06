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

	"log"
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

type Raftstate int

const (
	Follower  Raftstate = 0
	Candidate Raftstate = 1
	Leader    Raftstate = 2
)

const (
	HearbeatMs           = 100 // leader 100ms发起一次心跳
	ElectionTimeoutMinMs = 250 // 最小的选举时间
	ElectionTimeOutMaxMs = 400 // 最大的选举时间
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
	state Raftstate // Follower, Candidate, Leader
	// 2A:只考虑选举情况
	// 在当前任期内，此节点将选票投给了谁。
	//一个任期内，节点只能将选票投给某一个节点。因此当节点任期更新时要将 votedfor 置为 null。
	currentTerm int // 此节点的任期
	votedFor    int // 当前任期投票

	electionTime time.Time // 选举超时时间
}

type Log struct {
	term    int         // 该记录首次被创建时的任期号
	index   int         // 该记录在日志中的位置
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
	// 2A:只考虑选举情况
	Term        int // Candidate的任期
	CandidateId int // candidate请求投票
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// 2A:只考虑选举情况
	Term        int  // 此节点的任期。假如 Candidate 发现 Follower 的任期高于自己，则会放弃 Candidate 身份并更新自己的任期。
	VoteGranted bool // 是否同意candidated当选，true表示同意
}

//
// example RequestVote RPC handler.
// 请求投票RPC
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%d 在 term: %d 收到了来自 %d 的RequestVote request , args: %+v", rf.me, rf.currentTerm, args.CandidateId, args)
	// rpc handler 通用的规则: args 中 term 比 当前小，直接返回
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// rpc handler 通用规则: args 中的 term 比当前 server 大，当前 server 更新为 term，转成 follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.toFollower()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		log.Printf("%d 在 term %d 给 %d 投票", rf.me, rf.currentTerm, args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
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
	log.Printf("%d 在 term %d 收到 %d 的 AppendEntries request %+v", rf.me, rf.currentTerm, args.LeaderId, args)
	// rpc handler 通用的规则: args 中 term 比 当前小，直接返回
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// rpc handler 通用规则: args 中的 term 比当前 server 大，当前 server 更新为 term，转成 follower
	if args.Term > rf.currentTerm {
		rf.toFollower()
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	// 收到来自当前leader的心跳，重置选举超时器
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm
	reply.Success = true
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

//此处设置election timeout在250ms - 400ms之间。
func (rf *Raft) resetElectionTimer() {
	electionTimeout := time.Duration(ElectionTimeoutMinMs+rand.Intn(ElectionTimeOutMaxMs-ElectionTimeoutMinMs)) * time.Millisecond
	t := time.Now()
	rf.electionTime = t.Add(electionTimeout)
}

// func (rf *Raft) electionTimeout() time.Duration {
// 	return time.Duration(150+rand.Intn(150)) * time.Millisecond
// }

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// 心跳检测
		if rf.loadState() == Leader {
			// rf.AppendEntries(true)
			rf.heartbeat()
		}
		// 已经过了选举时间，需要发起选举
		if rf.loadState() != Leader && time.Now().After(rf.loadelectionTime()) {
			// 发起选举
			rf.election()
		}
		time.Sleep(HearbeatMs * time.Millisecond)
	}
}
func (rf *Raft) loadState() Raftstate {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) loadelectionTime() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.electionTime
}

func (rf *Raft) toFollower() {
	log.Printf("%d 在 term %d 成为follower", rf.me, rf.currentTerm)
	rf.state = Follower
}

func (rf *Raft) toLeader() {
	log.Printf("%d 在 term %d 成为Leader", rf.me, rf.currentTerm)
	rf.state = Leader
}

// 成功候选人时候要重置选举超时时间
func (rf *Raft) toCandidate() {
	rf.currentTerm++
	log.Printf("%d 在 term %d 成为了Candidate然后发起了投票", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.resetElectionTimer()
}

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.toCandidate()
	voteCount := 1
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &RequestVoteReply{}
			// 不能以不是Candidate 的身份发送选票
			if rf.loadState() != Candidate {
				return
			}
			// rpc请求
			if ok := rf.sendRequestVote(i, &args, reply); !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 通用规则: reply 中 term 大于当前，转为 Follower
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.toFollower()
				return
			}
			// double check，因为有可能发起新一轮选举了，才收到这个消息
			if rf.state != Candidate || reply.Term != rf.currentTerm || reply.Term != args.Term {
				return
			}
			if reply.VoteGranted {
				voteCount++
				log.Printf("%d 在 term %d 收到了 %d 的投票，现在票数 %d", rf.me, rf.currentTerm, i, voteCount)
			}
			if voteCount > len(rf.peers)/2 {
				rf.toLeader()
			}
		}(i)
	}
}

// leader才可以发送心跳
func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	// 2B时要添加
	log.Printf("%d 在 term %d 触发心跳, state: %d, ", rf.me, rf.currentTerm, rf.state) // 共享变量访问 要加锁
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			args := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			reply := &AppendEntriesReply{}
			// 这个检查是因为当一个协程收到比当前 term 大的 reply，会转变成 follower. 后面协程不应该再以 leader 身份发送同步信息
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			if ok := rf.sendAppendEntries(i, args, reply); !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.toFollower()
				return
			}
			// double check
			if rf.state != Leader || reply.Term != rf.currentTerm || reply.Term != args.Term {
				return
			}
		}(i)
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
	rf.state = Follower
	rf.resetElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

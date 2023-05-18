package raft

import (
	"math/rand"
	"time"
)

const electionTimeout = 1 * time.Second

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
// 其他节点处理投票请求
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
		// 判断主节点的log是否为最新
		if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
			reply.VoteGranted = false
			return
		}
		DPrintf("%d 在 term %d 给 %d 投票, lastLogIndex : %d , lastLogTerm(-1表示日志还是空的) : % d", rf.me, rf.currentTerm, args.CandidateId, lastLogIndex, lastLogTerm)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// 给其它节点投票了才要重置超时时间，没投票不能重置超时时间
		rf.persist(false)
		rf.resetElectionTimeL()
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

// 候选节点发送投票请求
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

		// 指南上说，每次处理回复rpc的时候，一定要看是否还在同一个时期
		if rf.currentTerm != args.Term {
			return
		}
		if replys.VoteGranted {
			*voteCount++
			DPrintf("%d 在term %d 收到了 %d 的投票, 现在总共票数 %d", rf.me, rf.currentTerm, serverId, *voteCount)
			if *voteCount > len(rf.peers)/2 {
				rf.toLeaderL()
				rf.sendAppendEntriesToAllPeerL(true) // true表示发送的是心跳
			}
		}
	}
}

// Raft指南上说了重置超时时间只能在3个地方
// 1. 收到心跳。 2.给别的节点投票，只有投了才可以重置。 3.重新开始一轮选举时要重置。
// 指南没说，但是我觉得很重要也是我踩的一个大坑。4.最后是很容易忽略的就是Leader发送心跳的时候，自己的超时时间也要重置
func (rf *Raft) resetElectionTimeL() {
	t := time.Now()
	t = t.Add(electionTimeout)
	ms := rand.Int63() % 300
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.tick()
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		rf.resetElectionTimeL()
		rf.sendAppendEntriesToAllPeerL(true)
	}
	if time.Now().After(rf.electionTime) {
		rf.resetElectionTimeL()
		rf.electionL()
	}
}

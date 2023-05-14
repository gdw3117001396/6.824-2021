package raft

import "log"

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

	reply.Success = false
	reply.XLen = -1   // length of log
	reply.XTerm = -1  // term of conficting term
	reply.XIndex = -1 // index of first entry of Xterm

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

	// PrevLogIndex与当前log长度不匹配时，更新当前log长度到Xlen
	if rf.log.lastindex() < args.PrevLogIndex || args.PrevLogIndex < rf.log.start() {
		DPrintf("%d 在 term %d 收到来自 %d 的日志不匹配1, rf.log: %+v", rf.me, rf.currentTerm, args.LeaderId, rf.log)
		reply.Success = false
		reply.Conflict = true
		reply.XLen = rf.log.lastindex() + 1
		return
	}
	// 当PreLogTerm与当前日志的任期不匹配时，找出日志第一个不匹配任期的index
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
		// reply.XLen = rf.log.len()
		return
	}
	reply.Success = true
	DPrintf("%d 在 term %d 收到来自 %d 的AppendEntries, PrevLogIndex: %d , PrevLogTerm: %d ", rf.me, rf.currentTerm, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
	// 更新本地日志
	needPersist := false
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		// 3.日志冲突, 删除
		if index <= rf.log.lastindex() && rf.log.entry(index).Term != entry.Term {
			rf.log.cutend(index)
			rf.log = mkLog(rf.log.Logs, rf.log.start())
			needPersist = true
			// rf.persist()
			// DPrintf("%d 在 term %d 收到了日志冲突, 删除, 当前日志为: %+v", rf.me, rf.currentTerm, rf.log)
		}
		// 4.添加不存在的新的日志,Append any new entries not already in the log
		if index > rf.log.lastindex() {
			rf.log.append(args.Entries[i:]...)
			// rf.persist()
			needPersist = true
			// DPrintf("%d 在 term %d 收到日志追加, %+v .", rf.me, rf.currentTerm, rf.log)
			break
		}
	}
	if needPersist {
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastindex())
		rf.apply()
	}
}

func (rf *Raft) sendAppendEntriesToAllPeerL(heartBeat bool) {
	DPrintf("%d 在term %d 开始发送AppendEntries, nextIndex: %+v", rf.me, rf.currentTerm, rf.nextIndex)
	for i := range rf.peers {
		if i != rf.me {
			if rf.log.lastindex() >= rf.nextIndex[i] || heartBeat {
				rf.sendAppendEntriesToPeerL(i, heartBeat)
			}
		}
	}
}
func (rf *Raft) sendAppendEntriesToPeerL(serverid int, heartBeat bool) {
	next := rf.nextIndex[serverid]
	//  跳过entry "0", 若leader安装了snapshot，会出现rf.log.start() > next的情况。
	if next <= rf.log.start() {
		next = rf.log.start() + 1
	}
	// // 当next的修改来自节点自身log长度时，是有可能大于rf.log.lastindex()的。
	if next-1 > rf.log.lastindex() {
		next = rf.log.lastindex()
	}
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
			rf.processAppendReplyL(serverid, args, reply)
		}
	}()
}
func (rf *Raft) processAppendReplyL(serverid int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%d 在 term %d 收到 %d 的AppendEntries reply %+v", rf.me, rf.currentTerm, serverid, reply)
	if reply.Term > rf.currentTerm {
		rf.toFollowerL(reply.Term)
	}
	// 看看是否还是同一个时期
	if args.Term != rf.currentTerm {
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
	} else {
		// 收到了重复且过期的请求，无需处理。
		if args.PrevLogIndex+1 != rf.nextIndex[serverid] && args.PrevLogIndex != rf.log.start() {
			return
		}

		if reply.XTerm == -1 {
			rf.nextIndex[serverid] = reply.XLen
			DPrintf("%d 在 term %d 拒绝了 %d 的日志传送，日志条目太少, nextIndex[%d]: %d .", serverid, rf.currentTerm, rf.me, serverid, rf.nextIndex[serverid])
		} else {
			lastLogInXTermIndex := rf.findLastLogInTerm(reply.XTerm)
			if lastLogInXTermIndex == -1 {
				// Leader 中没有 XTerm，nextIndex = XIndex
				rf.nextIndex[serverid] = reply.XIndex
				DPrintf("%d 在 term %d 拒绝了 %d 的日志传送, 没有Xterm, nextIndex[%d]: %d .", serverid, rf.currentTerm, rf.me, serverid, rf.nextIndex[serverid])
			} else {
				// Leader 有 XTerm，nextIndex = leader's last entry for XTerm
				rf.nextIndex[serverid] = lastLogInXTermIndex
				DPrintf("%d 在 term %d 拒绝了 %d 的日志传送,有Xterm,nextIndex[%d]: %d .", serverid, rf.currentTerm, rf.me, serverid, rf.nextIndex[serverid])
			}
		}
		// 如果follower太落后了就要发送快照了
	}
	rf.leaderCommitL()
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

func (rf *Raft) leaderCommitL() {
	if rf.state != Leader {
		log.Fatalf("advanceCommit: state %v\n", rf.state)
		return
	}
	start := rf.commitIndex + 1
	if start < rf.log.start() {
		start = rf.log.start() + 1
	}
	for n := start; n <= rf.log.lastindex(); n++ {
		if rf.log.entry(n).Term != rf.currentTerm {
			continue
		}
		matchIndexCount := 1
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if serverId != rf.me && rf.matchIndex[serverId] >= n {
				matchIndexCount++
			}
		}
		if matchIndexCount > len(rf.peers)/2 {
			rf.commitIndex = n
		}
	}
	rf.apply()
}

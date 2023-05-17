package raft

type InstallSnapshotargs struct {
	Term              int    // leader的任期
	LeaderId          int    // leader的ID，这样follower才能重定向客户端的请求
	LastIncludedIndex int    // 最后一个被快照取代的日志条目的索引
	LastIncludedTerm  int    // LastIncludedIndex所处的任期号
	Data              []byte // 从偏移量offset开始快照块的原始字节数据
	// Offset            int // 数据块在快照文件中位置的字节偏移量
	// Done              bool   // 是否是最后一个快照
}
type InstallSnapshotreplys struct {
	Term int // 当前的任期，供Leader自我更新
}

// 主节点发送快照
func (rf *Raft) sendSnapshot(serverId int, args *InstallSnapshotargs) {
	reply := &InstallSnapshotreplys{}
	ok := rf.sendInstallSnapshot(serverId, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.toFollowerL(reply.Term)
		}
		// 看看是否还是同一个时期
		if args.Term != rf.currentTerm {
			return
		}
		rf.nextIndex[serverId] = args.LastIncludedIndex + 1
		rf.matchIndex[serverId] = args.LastIncludedIndex
	}
}

// 当一个follower接收并处理一个InstallSnapshotRPC时，它必须使用Raft将快照交给服务。紧接着服务马上就会调用CondInstallSnapshot来安装快照了
func (rf *Raft) InstallSnapshot(args *InstallSnapshotargs, reply *InstallSnapshotreplys) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.toFollowerL(args.Term)
	}
	DPrintf("%d 在term %d 接收到了InstallSnapshot, args : %v ", rf.me, rf.currentTerm, args)
	rf.resetElectionTimeL()
	if args.LastIncludedIndex > rf.lastApplied && args.LastIncludedIndex > rf.snapshotIndex && rf.waitingSnapshot == nil {
		rf.waitingSnapshot = args.Data
		rf.waitingIndex = args.LastIncludedIndex
		rf.waitingTerm = args.LastIncludedTerm
		rf.apply()
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// 这个函数是在InstallSnapshotRPC发送了ApplyMsg之后马上调用
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2D).
	// 旧的日志不按照
	if lastIncludedIndex <= rf.lastApplied || lastIncludedIndex <= rf.snapshotIndex {
		return false
	}
	DPrintf("%d 在term %d 收到condinstallsnapshot: lastIncludedIndex %d lastIncludedTerm %d", rf.me, rf.currentTerm, lastIncludedIndex, lastIncludedTerm)
	if lastIncludedIndex >= rf.log.lastindex() || lastIncludedTerm != rf.log.entry(lastIncludedIndex).Term {
		// 1.lastIncludedIndex >= rf.log.lastindex() 表明日志太落后了
		// 2.日志不匹配也是直接清空
		rf.log = mkLog(make([]Entry, 1), lastIncludedIndex)
		rf.log.entry(lastIncludedIndex).Term = lastIncludedTerm
	} else {
		// 清空过期日志，保留后续内容
		rf.log.cutstart(lastIncludedIndex - rf.log.start())
		rf.log = mkLog(rf.log.Logs, rf.log.start())
	}
	rf.snapshotIndex = lastIncludedIndex
	rf.snapshotTerm = lastIncludedTerm
	rf.snapshot = snapshot
	rf.persist()
	rf.lastApplied = lastIncludedIndex
	if lastIncludedIndex > rf.commitIndex {
		rf.commitIndex = lastIncludedIndex
	}
	DPrintf("%d 在term %d condinstallsnpasot_res: log start: %v item: %v lastindex: %v", rf.me, rf.currentTerm, rf.log.start(), rf.log.entry(rf.log.start()), rf.log.lastindex())
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 这个是上层服务要raft安装快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 拒绝旧的快照安装请求
	if index <= rf.snapshotIndex {
		return
	}
	rf.snapshot = snapshot
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.log.entry(index).Term
	DPrintf("%d 在term %d 压缩日志了 index:%d log startindex:%d lastindex:%d", rf.me, rf.currentTerm, index, rf.log.start(), rf.log.lastindex())
	// 清空过期日志
	rf.log.cutstart(index - rf.log.start())
	rf.log = mkLog(rf.log.Logs, rf.log.start())
	rf.persist()
}

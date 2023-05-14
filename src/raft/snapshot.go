package raft

type InstallSnapshotargs struct {
	Term              int // leader的任期
	LeaderId          int // leader的ID，这样follower才能重定向客户端的请求
	LastIncludedIndex int // 最后一个被快照取代的日志条目的索引
	LastIncludedTerm  int // LastIncludedIndex所处的任期号
	Offset            int // 数据块在快照文件中位置的字节偏移量
	// Data              []byte // 从偏移量offset开始快照块的原始字节数据
	// Done              bool   // 是否是最后一个快照
}
type InstallSnapshotreplys struct {
	Term int // 当前的任期，供Leader自我更新
}

// 这个是上层应用保存好数据后告诉raft要进行日志压缩了
func (rf *Raft) InstallSnapshot(args *InstallSnapshotargs, reply *InstallSnapshotreplys) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// 这个函数是在InstallSnapshotRPC发送了ApplyMsg之后马上调用
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

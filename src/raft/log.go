package raft

type Entry struct {
	Term    int         // 该记录首次被创建时的任期号
	Command interface{} // 命令
}

type Log struct {
	log    []Entry
	index0 int // 日志起始index
}

func mkLogEmpty() Log {
	return Log{make([]Entry, 0), 0}
}

func mkLog(log []Entry, index0 int) Log {
	return Log{log, index0}
}

func (l *Log) append(e ...Entry) {
	l.log = append(l.log, e...)
}

// 开始的index
func (l *Log) start() int {
	return l.index0
}

// 切断自己的
func (l *Log) cutend(index int) {
	l.log = l.log[0 : index-l.index0]
}

// 切断自己的
func (l *Log) cutstart(index int) {
	l.index0 += index
	l.log = l.log[index:]
}

//从index 拷贝到尾
func (l *Log) slice(index int) []Entry {
	return l.log[index-l.index0:]
}

func (l *Log) lastindex() int {
	return l.index0 + len(l.log) - 1
}

func (l *Log) entry(index int) *Entry {
	return &(l.log[index-l.index0])
}

func (l *Log) lastentry() *Entry {
	return l.entry(l.lastindex())
}

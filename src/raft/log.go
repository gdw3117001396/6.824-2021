package raft

type Entry struct {
	Term    int         // 该记录首次被创建时的任期号
	Command interface{} // 命令
}

type Log struct {
	Logs   []Entry
	Index0 int // 日志起始index
}

func mkLogEmpty() Log {
	return Log{make([]Entry, 0), 0}
}

func mkLog(log []Entry, index0 int) Log {
	return Log{log, index0}
}

func (l *Log) append(e ...Entry) {
	l.Logs = append(l.Logs, e...)
}

// 开始的index
func (l *Log) start() int {
	return l.Index0
}

// 切断自己的
func (l *Log) cutend(index int) {
	l.Logs = l.Logs[0 : index-l.Index0]
}

// 切断自己的
func (l *Log) cutstart(index int) {
	l.Index0 += index
	l.Logs = l.Logs[index:]
}

//从index 拷贝到尾
func (l *Log) slice(index int) []Entry {
	return l.Logs[index-l.Index0:]
}

func (l *Log) lastindex() int {
	return l.Index0 + len(l.Logs) - 1
}

func (l *Log) entry(index int) *Entry {
	return &(l.Logs[index-l.Index0])
}

func (l *Log) lastentry() *Entry {
	return l.entry(l.lastindex())
}

func (l *Log) len() int {
	return len(l.Logs)
}

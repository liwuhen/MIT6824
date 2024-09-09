package raft 

// LogEntries 结构体用于存储日志条目的相关信息。
// 每个日志条目代表了服务器接收到的一个命令及其元数据。
type LogEntries struct {
	Command interface{}  // Command 字段存储日志条目关联的具体命令内容。这是一个接口类型，允许存储任意类型的命令数据。
	Term    int // Term 表示该日志条目是在哪个任期（Term）中创建或复制的。
	Index   int // Index 表示该日志条目在日志中的序列号（索引）。索引用于唯一标识每一条日志，并且在日志匹配和日志压缩等操作中起到核心作用。
}

// RequestVote RPC 请求参数结构体
type RequestVoteArgs struct {
	Term         int // 候选人的当前任期
	CandidateId  int // 请求投票的候选人 id
	LastLogIndex int // 候选人最后一个日志条目的索引
	LastLogTerm  int // 候选人最后一个日志条目任期
}

// RequestVote RPC 响应结构体
type RequestVoteReply struct {
	Term        int  // 当前任期，用于候选人更新自身的任期
	VoteGranted bool // 表示是否投票给了候选人，true 表示候选人获得了投票。
}

// AppendEntriesArgs 日志条目/心跳的 RPC 请求参数结构。
type AppendEntriesArgs struct {
	Term         int // 领导者的当前任期
	LeaderId     int // 领导者的 ID
	PrevLogIndex int // 新日志条目之前的日志条目的索引
	PrevLogTerm  int // 新日志条目之前的日志条目的任期
	Entries      []LogEntries // 要存储的日志条目（为空表示心跳，可以发送多个以提高效率）
	LeaderCommit int // 领导者的 commitIndex      
}

// AppendEntriesReply 日志条目/心跳的 RPC 响应结构。
type AppendEntriesReply struct {
	Term          int  // 当前任期，用于领导者更新自己的任期
	Success       bool // 如果跟随者包含了匹配的日志条目且日志条目已成功存储，则为 true
	ConflictTerm  int  // 在跟随者日志中与领导者发送的日志条目发生冲突的那条日志的任期号
	ConflictIndex int  // 在跟随者日志中发生冲突的具体条目的索引。索引是日志条目在日志文件中的索引
}


// InstallSnapshotArgs 定义了在 Raft 协议中安装快照所需参数的结构。
type InstallSnapshotArgs struct {
	Term              int    // 是发送快照的 leader 的任期。
	LeaderId          int    // 是 leader 的标识符，便于 follower 将 client 重定向到 leader。
	LastIncludedIndex int    // 快照替换的最后一个条目的 index。这个索引及其之前的所有日志条目都将被快照替换。
	LastIncludedTerm  int    // LastIncludedIndex 所属的任期。它确保快照包含最新的信息。
	SnapshotData      []byte // Data 是表示快照数据的原始字节切片。快照以分块的方式发送，从指定的偏移量开始。
}

// InstallSnapshotReply 定义了跟随者对快照安装请求的响应结构。
type InstallSnapshotReply struct {
	Term   int  // RPC 接收 server 的 current term，leader 更新自己用
	Accept bool // follower 是否接受这个快照
}


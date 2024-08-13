package raft 



// RequestVote RPC 请求参数结构体
type RequestVoteArgs struct {
	Term         int // 候选人的当前任期
	CandidateId  int // 请求投票的候选人id
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
	LeaderCommit int // 领导者的 commitIndex
}

// AppendEntriesReply 日志条目/心跳的 RPC 响应结构。
type AppendEntriesReply struct {
	Term    int  // 当前任期，用于领导者更新自己的任期
	Success bool // 如果跟随者包含了匹配的日志条目且日志条目已成功存储，则为 true
}

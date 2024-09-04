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
	"bytes"
	"sync"
	// "sort"
	"sync/atomic"
	"time"
	// "fmt"
	"6.824/labgob"
	"6.824/labrpc"
)

/*ApplyMsg 有两种用途，一种是运送要 apply 的指令，一种是运送要安装的快照。
  raft server 发送 ApplyMsg 给应用层的 channel。在 lab2 中只需要根据日志
  条目生成 msg 然后发送到 applyCh 即认为是日志 apply 了   */
type ApplyMsg struct {
	CommandValid bool         // 当 ApplyMsg 用于 apply 指令时为 true，其余时候为 false
	Command      interface{}  // 要 apply 的指令
	CommandIndex int          // 要 apply 的指令的 index

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}


// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers  集群消息
	persister *Persister          // Object to hold this peer's persisted state  持久化
	me        int                 // this peer's index into peers[]  当前节点 id
	dead      int32               // set by Kill()  是否死亡，1 表示死亡，0 表示还活着

	// persistent state on all servers
	// 2A
	serverState       RoleType                // 当前节点状态
	currentTerm       int                     // 节点当前任期
	votedFor          int                     // follower 把票投给了哪个 candidate

	// 2B
	commitIndex       int            // 记录已知被提交的最高日志条目索引（初始化为 0，单调递增）
	lastApplied       int            // 记录已应用于状态机的最高日志条目索引（初始化为 0，单调递增）
	logs			  [] LogEntries  // 日志条目集合；每个条目包含状态机的命令及该条目被领导者接收时的任期（日志的首个索引为 1）
	nextIndex	      []int          // 针对每个服务器，记录下一个将发送给该服务器的日志条目索引（初始化为领导者最后日志索引 +1）
	matchIndex        []int          // 针对每个服务器，记录已知在该服务器上复制的最高日志条目索引（初始化为 0，单调递增）
	applyChan         chan ApplyMsg  // 通道，用于提交给客户端已经完成超过半数服务器复制成功的 log 处理结果

	timer             *time.Timer    // 计时器指针
	electionFlag      bool           // Candidate 是否成功竞选 leader

}

/* 返回当前的任期和该服务器是否认为自己是领导者。 
   该方法用于外部查询 Raft 服务器的当前状态*/
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.serverState == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	// 序列化完成后，从缓冲区获取字节切片准备存储
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)

	// var currentTerm int
	// var votedFor int
	// var logs []LogEntries

	// if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
	// 	DPrintf("Raft server %d readPersist ERROR!\n", rf.me)
	// } else {
	// 	rf.currentTerm = currentTerm
	// 	rf.votedFor    = votedFor
	// 	rf.logs        = logs
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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


/*  处理 RequestVote RPC 请求的处理函数 
任期比较：
	如果请求者的任期大于当前节点的任期，说明请求者的信息更新，当前节点需要更新其任期并转换为 Follower 角色。
	如果请求者的任期小于当前节点的任期，则当前节点拒绝投票，因为其任期更大，更“新”。*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 锁定当前 Raft 实例，以保证并发安全。
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer rf.persist()

	if args.Term > rf.currentTerm { // 如果请求者的任期大于当前节点的任期，则更新当前节点的任期并转换为 Follower 角色。
		rf.ConverToFollower(args.Term)
	} 

	if args.Term < rf.currentTerm { 
		// 如果请求者的任期小于当前节点的任期，或者在同一任期内已经投给了其他候选人，
		// 则直接拒绝投票并返回当前任期与投票结果。
		reply.Term        = rf.currentTerm
		reply.VoteGranted = false
		return
	} 

	// DPrintf("Server %d gets an RequestVote RPC with a higher term from Candidate %d, and its current term become %d.\n", 
	// 	rf.me, args.CandidateId, rf.currentTerm)

	// 选举投票限制条件：
	// 1-候选人最后一条 Log 条目的任期号大于本地最后一条 Log 条目的任期号；
	// 2-或者，候选人最后一条 Log 条目的任期号等于本地最后一条 Log 条目的任期号，且候选人的 Log 记录长度大于等于本地 Log 记录的长度
	lastLog := rf.logs[len(rf.logs)-1]
	if args.LastLogTerm < lastLog.Term || 
		(args.LastLogTerm == lastLog.Term && args.LastLogIndex < lastLog.Index) {
		// DPrintf("Candidate %d fail", args.CandidateId)
		return
	}

	// 如果当前节点是 Follower，且未投给其他候选人或已投票给该候选人。
	if rf.serverState == Follower && 
		(rf.votedFor == noVoted || rf.votedFor == args.CandidateId) {
		rf.votedFor       = args.CandidateId   // 更新投票给的候选人 ID。
		reply.VoteGranted = true
		
		rf.timer.Stop()
		rf.timer.Reset(time.Duration(getRandTime(300, 500)) * time.Millisecond)
	} else {
		reply.VoteGranted = false  // 包含在同一任期内已经投给了其他候选人，则直接拒绝投票并返回当前任期与投票结果。
	}

	// 设置返回的任期
	reply.Term = rf.currentTerm
}

// sendRequestVote 发送 RequestVote RPC 给服务器的示例代码。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	// server 下标节点调用 RequestVote RPC 处理程序
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		// DPrintf("=>[DEBUG]: server %d call failed serverState %d \n", rf.me, rf.serverState)
		return false
	}

	// 如果该节点已经不是候选者或者该节点请求时的任期与当前任期不一致，直接返回 true，无须继续拉票
	rf.mu.Lock()
	if rf.serverState != Candidate || rf.currentTerm != args.Term{
		rf.mu.Unlock()
		return true
	}

	// 如果收到的回复中的任期比当前节点的任期大，遇到了任期比自己大的节点，转换为跟随者 follower
	if reply.Term > rf.currentTerm {
		rf.ConverToFollower(reply.Term)
		rf.mu.Unlock()

		// 重置计时器（选举超时时间）
		rf.timer.Stop()
		rf.timer.Reset(time.Duration(getRandTime(300, 500)) * time.Millisecond)
		return true
	}

	rf.mu.Unlock()

	return true
}

/* 向其他 Raft 节点并行发送投票请求的 RPC。
*/
func (rf *Raft) sendAllRaftRequestVote() {

	rf.mu.Lock()
	rf.electionFlag = false // 不管是 follower 第一次参选还是 candidate 再次参选，只要参加竞选就将 electionFlag 设为 false 以便超时后的等待判定
	lastLog := rf.logs[len(rf.logs)-1]

	// 构建请求投票的参数
	args := &RequestVoteArgs {
		Term: 			rf.currentTerm, // 当前任期
		CandidateId: 	rf.me,          // 候选人 ID
		LastLogIndex:   lastLog.Index,  // 候选人最后一个日志条目的索引（暂时设置为 0）
		LastLogTerm:    lastLog.Term,   // 候选人最后一个日志条目的任期（暂时设置为 0）
	}
	rf.mu.Unlock()

	votes    := 1
	finished := 1                 // 收到的请求投票回复数（自己的票也算）
	var voteMu sync.Mutex
	cond := sync.NewCond(&voteMu) // 将条件变量与锁关联

	// 向所有其他节点发送请求投票的 RPC
	for index := range rf.peers {

		if rf.killed() { // 如果在竞选过程中 Candidate 被 kill 了就直接结束
			// DPrintf("Candidate %d is dead !\n", rf.me)
			return 
		}

		rf.mu.Lock()
		state := rf.serverState
		rf.mu.Unlock()

		// 向除当前节点外的其他节点发送 RPC，且当前节点为候选者状态
		if index != rf.me && state == Candidate {
			// 并行发送请求投票的 RPC
			go func(id int) {
				// 构建返回参数
				ret := &RequestVoteReply {
					Term:        0,
					VoteGranted: false,
				}
				ok := rf.sendRequestVote(id, args, ret)
				if !ok {
					// DPrintf("Candidate %d call server %d for RequestVote failed!\n", rf.me, id)
				}

				rf.mu.Lock()
				serverState := rf.serverState
				VoteGranted := ret.VoteGranted
				rf.mu.Unlock()

				voteMu.Lock()
				// 如果收到投票，且当前节点仍是候选者，增加投票计数
				if VoteGranted && serverState == Candidate{
					votes++
				}
				finished++;
				voteMu.Unlock()
				cond.Broadcast()
			}(index)
		}
	}

	sumNum := len(rf.peers)     // 集群中总共的 server 数
	majorityNum := sumNum/2 + 1 // 满足大多数至少需要的 server 数量

	voteMu.Lock()
	for votes < majorityNum && finished != sumNum { // 投票数尚不够，继续等待剩余 server 的投票
		cond.Wait() // 调用该方法的 goroutine 会被放到 Cond 的等待队列中并阻塞，直到被 Signal 或者 Broadcast 方法唤醒
		rf.mu.Lock()
		stillCandidate := (rf.serverState == Candidate)
		rf.mu.Unlock()
		if !stillCandidate {
			voteMu.Unlock()
			return
		}
	}

	// 如果当前节点的投票数超过一半，且节点仍为候选者，转为 Leader
	if votes >= majorityNum {
		rf.ConverToLeader()   // 成为 leader 就不需要超时计时了，直至故障或发现自己的 term 过时
	} else { // 收到所有回复但选票仍不够的情况，即竞选失败
		// DPrintf("Candidate %d failed in the election and continued to wait...\n", rf.me)
	}
	voteMu.Unlock()

}

/*处理来者 Leader 的 AppendEntries 的 RPC 请求，处理接收到的 AppendEntries 请求，包括心跳和日志条目的复制*/
func (rf * Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer rf.persist()

	// DPrintf("Server %d gets an AppendEntries RPC(term:%d, Entries len:%d) with a higher term from Leader %d, and its current term become %d.\n",
	// 	rf.me, args.Term, len(args.Entries), args.LeaderId, rf.currentTerm)

	/*============2A-选举============*/
	// 当前请求的 Term 小于当前节点的 Term，那么说明收到了来自过期的领导人的附加日志请求，那么拒接处理。
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term    = rf.currentTerm // 将自己的 current term 附在回复中
		return 
	}

	// 收到 Leader 更高的任期时，更新自己的任期，转为 leader 的追随者 
	// 或者当前节点是 candidate，且任期等于 leader 任期（表明节点中存在多个 Candidate），转为候选者。
	if rf.currentTerm < args.Term || (rf.currentTerm == args.Term && rf.serverState == Candidate){
		rf.ConverToFollower(args.Term)

		// 如果 follower 的 term 与 leader 的 term 相等（大多数情况），那么 follower 收到 AppendEntries RPC 后也需要重置计时器
		rf.timer.Stop()
		rf.timer.Reset(time.Duration(getRandTime(300, 500)) * time.Millisecond)

		if rf.serverState == Leader {
			DPrintf("Leader %d (term:%d) tranfer to follower (New Leader %d term:%d).\n",rf.me, rf.currentTerm, args.LeaderId, args.Term)
		}
	}

	// DPrintf("Server %d gets an AppendEntries RPC(term:%d, Entries len:%d) with a higher term from Leader %d, and its current term become %d.\n",
	// 	rf.me, args.Term, len(args.Entries), args.LeaderId, rf.currentTerm)

	/*============2B-新增日志复制功能============*/
	// 如果 preLogIndex 的长度大于当前的日志的长度，那么说明跟随者缺失日志。
	// 情况一：leader 请求的 PrevLogIndex 在 follower 中不存在，大于 follower 的日志长度。
	if args.PrevLogIndex >= len(rf.logs) {
		/* 领导者和跟随者之间的日志出现了不一致，或者跟随者落后很多且领导者的信息过时。领导者收到这样的失败响应后，会根据跟随者的反馈调整其 nextIndex 值，
		然后重试发送 AppendEntries 请求，从一个更早的索引开始，以解决日志不一致的问题。这样，通过一系列的尝试与调整，Raft 算法能够最终确保集群间日志的一致性。
		*/
		reply.ConflictIndex = rf.logs[len(rf.logs)-1].Index + 1
		reply.ConflictTerm  = -1
		
		reply.Success = false // 返回 false
		reply.Term    = rf.currentTerm

		return
	}

	// 优化逻辑：处理日志条目任期不匹配的情况，那么说明跟随者的日志不缺失。
	// 情况二：leader 请求的 PrevLogIndex 在 follower 中存在，但是 follower 中 PrevLogIndex 处的 Term 在与 Leader 的 PrevLogTerm 不同。
	if args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		// Follower 处存在 PrevLogIndex，记录 follower 中 PreLogIndex 位置处任期号。
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
		// 然后在 follower 的日志中搜索任期等于 conflictTerm 的第一个条目索引。
		for index := args.PrevLogIndex; index >= 0; index-- {
			// 遍历寻找 Follower 的任期与 PrevLogIndex 处任期不一致的条目时，记录其下一个条目的索引作为冲突索引。
			if rf.logs[index].Term != rf.logs[args.PrevLogIndex].Term {
				reply.ConflictIndex = index + 1
				break
			}
		}
		reply.Success = false // 返回 false
		reply.Term    = rf.currentTerm
		return 
	}

	// 如果请求的 PrevLogIndex 与 Term 在 follower 中存在，表明匹配到了两个日志一致的最新日志条目

	if args.PrevLogIndex + len(args.Entries) <= rf.commitIndex {
		return
	}

	// 情况三：leader 请求的 PrevLogIndex 在 follower 中存在，且 follower 中 PrevLogIndex 处的 Term 在与 Leader 的 PrevLogTerm 相同。
	// 分析：3.1-表明 leader 的日志长度可能与 follower 的日志长度相等。3.2-表明 leader 的日志长度可能大于 follower 的日志长度。
	// 针对情况三需要考虑 follower 日志在 PrevLogIndex 之后的日志条目与 leader 发送的 Entries 是否 index 与 term 相同。
	// 分为两种：相同与不相同的情况。
	for index := 0; index < len(args.Entries); index++ {
		// 计算 leader 请求的 Entries 中当前条目索引在 follower 日志中的目标索引位置
		currentIdx := index + args.PrevLogIndex + 1 // Entries 中条目在 follower 日志条目的索引偏移量
		// 1-针对 3.1;判断当前条目索引是否超出 follower 日志的长度，若超出直接在 follower 日志的尾部 currentIdex 后添加 leader 发送的 entries
		// 2-针对 3.2;判断当前条目的任期是否与 follower 在 currentIdx 处日志的任期相同，若不同，表明 index 相同，term 不同，表明 leader
		// 请求的添加的日志在 follower 中已经存在日志中冲突。
		if currentIdx >= len(rf.logs) || rf.logs[currentIdx].Term != args.Entries[index].Term {
			// 如果存在落后或者冲突，以当前领导者的日志为主，从当前位置开始，用领导者发送的日志条目替换或追加到跟随者日志中
			// 这里使用切片操作，保留原有日志的前半部分，然后追加领导者发送的日志条目
			newLog := rf.logs[:currentIdx]
			newLog  = append(newLog, args.Entries[index:]...)
			rf.logs = newLog
			// 替换或追加完毕后，跳出循环
			break
		}
	}	

	// 更新 commitIndex
	// 保持一致性：args.LeaderCommit 是领导者告知的已提交日志的最高索引。跟随者需要确保自己的 commitIndex 至少达到这个值，
	// 以保证整个集群的一致性。如果跟随者的日志足够新，能够包含领导者所提交的所有日志条目，
	// 那么跟随者的 commitIndex 就应该更新为 args.LeaderCommit，以反映集群的最新一致状态。
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logs[len(rf.logs)-1].Index)
	}

	// fmt.Printf("server %d follower logs len : %d \n", rf.me, len(rf.logs))
	reply.Success = true
	reply.Term    = rf.currentTerm
}

/*向指定的节点发送 AppendEntries RPC 请求，并处理响应。*/
func (rf * Raft) sendAppendEntries(id int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	if ok := rf.peers[id].Call("Raft.AppendEntriesHandler", args, reply); !ok {
		// 发送失败直接返回即可。
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 2A-选举
    // 如果当前节点不再是领导者，则直接返回 || 阻止过时 RPC：leader 发送的请求的任期与当前任期不同，则直接返回
    if rf.serverState != Leader || args.Term != rf.currentTerm{
       return false
    }

    // 如果响应中的任期大于当前任期，当前节点会转换为跟随者
    if reply.Term > rf.currentTerm {
		// DPrintf("Leader %d (term:%d) tranfer to follower (reply term:%d).\n",rf.me, rf.currentTerm, reply.Term)
		rf.ConverToFollower(reply.Term)
		return false
    }

	// 2B-新增日志复制功能

	// 当接收的 AppendEntries RPC 响应失败，意味着跟随者上的日志与领导者尝试追加的日志条目之间存在不一致。
	// leader 通过重复发送 AppendEntries 的方式解决：
	// 情况 1-响应的冲突任期为 -1：表明 leader 发送的 append 信息 preLogIndex 的长度大于当前的日志的长度，那么说明跟随者缺失日志
	// 解决：调整 leader 的 nextindx 的日志条目索引。nextindex = ConflictIndex
	// 情况 2-响应的冲突任期不为 -1：表明 leader 与 follower 的日志条目存在任期不匹配的情况，且跟随者的日志不缺失。
	// 解决：搜索 Leader 日志中任期为 ConfictTerm 的条目，若存在，修改 nextindex 为其日志中此任期的最后一个条目的索引 +1。
	// 若不存在，修改 nextindex 为响应的 ConflictIndex，重置 follower 中此任期的所有日志信息。
	if !reply.Success {
		if reply.ConflictTerm == -1 {
			rf.nextIndex[id] = reply.ConflictIndex
		} else {
			flag := true
			for index := rf.logs[len(rf.logs)-1].Index; index >= 0; index-- {
				// 找到冲突任期的最后一条日志（冲突任期号为跟随者日志中最后一条条目的任期号）
				if rf.logs[index].Term == reply.ConflictTerm {
					rf.nextIndex[id] = index + 1
					flag = false
					break
				} 
				if rf.logs[index].Term < reply.ConflictTerm {
					break
				}
			}

			if flag {
				// 如果没有找到冲突任期，则设置 nextIndex 为冲突索引
				rf.nextIndex[id] = reply.ConflictIndex
			}
		}
	} else {
		// 同步成功，根据 guide，你不能假设 server 的状态在它发送 RPC 和收到回复之间没有变化。
		// 因为可能在这期间收到新的指令而改变了 log 和 nextIndex
		possibleMatchIdx := args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[id]  = max(possibleMatchIdx + 1, rf.nextIndex[id])

		// 更新 matchIndex[id] 为最新的已知被复制到跟随者的日志索引
		rf.matchIndex[id] = max(possibleMatchIdx, rf.matchIndex[id])

		rf.checkCommitIndex()
	}
	return true
}

/*领导者向其他所有节点发送附加日志条目（或心跳）请求。在领导者周期性地发送心跳或需要复制日志条目到所有节点时使用*/
func (rf *Raft) sendAllRaftAppendEntries() {

	for server := range rf.peers {

		if rf.killed() { return }

		rf.mu.Lock()
		state := rf.serverState
		rf.mu.Unlock()
		// 对于每个不是当前节点的节点，leader 启动一个新的 goroutine 来发送 AppendEntries 请求
		if server != rf.me && state == Leader {
			rf.mu.Lock()
			nextId  := rf.nextIndex[server] // 获取 Leader 对于第 id 个服务器的下一个要发送的日志 id
			lastLog := rf.logs[nextId-1]    // 获取上次发送给 follower 的最后一条日志条目
			var logs []LogEntries

			// 如果 leader 的日志从 nextIdx 开始有要发送的日志，则此 AppendEntries RPC 需要携带从 nextIdx 开始的日志条目
			if rf.logs[len(rf.logs)-1].Index >= nextId {
				logs = make([]LogEntries, len(rf.logs)-nextId) // 创建一个新切片，用于存放需要同步给 follower 的尚未同步的日志条目
				copy(logs, rf.logs[nextId:])    // 拷贝尚未同步的日志
			} else {
				// 只发送心跳信号，不携带日志信息
				logs = []LogEntries {}
			}
			
			args := &AppendEntriesArgs {
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: lastLog.Index,
				PrevLogTerm:  lastLog.Term,
				Entries:      logs,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			go func(id int) {
				reply := &AppendEntriesReply {
					Term: 0,
					Success: false,
				}
				// rf.mu.Lock()
				// DPrintf("Leader %d sends AppendEntries RPC(term:%d, Entries len:%d, logs len:%d, nextId:%d | preIndex:%d, PreTerm:%d) to server %d...\n", 
				// 	rf.me, rf.currentTerm, len(args.Entries), len(rf.logs), nextId, args.PrevLogIndex, args.PrevLogTerm, id)
				// rf.mu.Unlock()
				if ok := rf.sendAppendEntries(id, args, reply); !ok {
					// 如果由于网络原因或者 follower 故障等收不到 RPC 回复（不是 follower 将回复设为 false）
					// 则 leader 无限期重复发送同样的 RPC（nextIndex 不前移），等到下次心跳时间到了后再发送
					// DPrintf("Leader %d calls server %d for AppendEntries or Heartbeat failed!\n", rf.me, id)
					return
				}
			}(server)
		}
	}
}

/*领导者节点根据 Raft 算法的规则检查并更新 commitIndex。*/
func (rf *Raft) checkCommitIndex() {
	// 从最后一条日志开始遍历，倒序遍历日志条目，快照保存的日志不需要检查
	for idx := len(rf.logs) - 1; idx >= rf.commitIndex; idx-- {
		// figure8 简化版本
		// Leader 不能直接提交不属于 Leader 任期的日志条目
		if rf.logs[idx].Term < rf.currentTerm || rf.serverState != Leader {
			return
		}
		count := 1
		// 遍历每个服务器的 matchIndex，如果大于等于当前遍历的日志索引，则表明该服务器已经成功复制日志，计数加 1
		for i := range rf.matchIndex {
			// 需要加上快照的最后一条索引
			if i != rf.me && rf.matchIndex[i] >= idx {
				count++
			}
		}
		// 如果计数大于半数，则更新 commitIndex 为当前遍历的日志索引，允许更多的日志条目被状态机应用。
		if count > len(rf.peers)/2 {
			if rf.serverState == Leader {
				// 在 leader 的日志索引 idx 下，leader 的日志成功附加到集群中的 N/2+1 的节点上，更新 leader 的 commitIndex
				rf.commitIndex = idx
				// DPrintf("Leader%d's commitIndex is updated to %d.\n", rf.me, idx)
			}
			break
		}
	}
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	// 检查是否有新的日志需要应用
	// 如果 commitIndex 不大于 lastApplied，说明没有新的日志需要应用
	if rf.commitIndex <= rf.lastApplied {
		rf.mu.Unlock()
		return
	} 

	// 创建一个新的日志条目切片，用于存储需要应用的日志，应用后就可以追上已提交的日志
	copyLogs := make([]LogEntries, rf.commitIndex-rf.lastApplied)
	// 复制需要应用的日志条目到 copyLogs 中
	// rf.lastApplied+1 是通过已经上次应用的日志索引获取这次需要应用的日志的起始索引
	copy(copyLogs, rf.logs[rf.lastApplied+1:rf.commitIndex+1])
	// 应用日志条目之后更新 lastApplied（最新已应用的日志索引）
	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()

	// 这里不要加锁 2D 测试函数会死锁
	// 遍历从 lastApplied+1 到 commitIndex 的所有日志条目
	for _, logEntity := range copyLogs {
		// 将每个条目的命令通过 applyChan 发送出去，以供状态机执行
		rf.applyChan <- ApplyMsg{
			CommandValid: true,              // 包含一个新提交的日志条目
			Command:      logEntity.Command, // 新提交的日志条目
			CommandIndex: logEntity.Index,   // 新提交日志条目的索引
		}
	}
}

// apply 方法负责将已知提交的日志条目应用到状态机中。
// 这个方法检查是否有新的日志条目可以被应用，并将它们发送到 applyChan，
// 以便状态机能够处理这些条目并更新其状态。
func (rf *Raft) applyStateMachine() {
	for !rf.killed() {   // 如果 server 没有被 kill 就一直检测
		// 确保已提交的日志条目被及时应用到状态机上
		rf.apply()
		time.Sleep(commitInterval)
	}
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term  := -1
	isLeader := true

	// Your code here (2B).
	if rf.serverState != Leader {
		// DPrintf("Client sends a new commad to Server %d but lt's not Leader!\n", rf.me)
		return -1, -1, false 
	}

	addLog := LogEntries {
		Command: command,
		Term:    rf.currentTerm,
		Index:   rf.logs[len(rf.logs)-1].Index + 1,
	}

	// 增加新条目到日志列表中
	rf.logs = append(rf.logs, addLog)
	index   = addLog.Index
	term    = addLog.Term

	// DPrintf("[Start]Client sends a new commad to Leader %d!\n", rf.me)
	// 客户端发来新的 command，复制日志到各 server，调用 sendAllRaftAppendEntries()
	// 如果追加失败（网络问题或日志不一致被拒绝），则重复发送由携带日志条目的周期性的心跳包来完成
	go rf.sendAllRaftAppendEntries()  // 由新日志触发 AppendEntries RPC 的发送

	return index, term, isLeader
}

/* Kill 测试器在每次测试后不会停止 Raft 创建的 goroutine，但它确实会调用 Kill() 方法。你的代码可以使用 killed() 来
   检查是否已调用 Kill()。使用 atomic 避免了对锁的需求。问题在于长时间运行的 goroutine 会使用内存并可能消耗 CPU 时间，
   这可能导致后续测试失败并生成令人困惑的调试输出。任何具有长时间运行循环的 goroutine 都应该调用 killed() 
   以检查它是否应该停止。Kill 将当前 Raft 实例标记为已终止。*/
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

/* killed 检查当前 Raft 实例是否已被终止。使用 atomic.LoadInt32(&rf.dead) 来获取 rf.dead 的值。
   如果值为 1，则表示该实例已被终止。*/
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/* ticker 协程在近期没有收到心跳的情况下启动新的选举。*/
func (rf *Raft) CheckTimeout() {

	for !rf.killed() {
		// 在这里添加代码以检查是否应该启动领导者选举
		// 并使用 time.Sleep() 随机化休眠时间。
		select {

		case <- rf.timer.C: // 当 timer 超时后会向 C 中发送当前时间，此时 case 的逻辑就会执行，从而实现超时处理
			rf.mu.Lock()
			role := rf.serverState
			rf.mu.Unlock()

			switch role { // 根据当前的角色来判断属于哪种超时情况，执行对应的逻辑

			case Follower:
				// 竞选前重置计时器（选举超时时间）
				rf.timer.Stop()
				rf.timer.Reset(time.Duration(getRandTime(300, 500)) * time.Millisecond)  // 增加扰动避免多个 Candidate 同时进入选举

				rf.ConverToCandidate()  // 转换成 candidate 宣布开始竞选
				go rf.sendAllRaftRequestVote()

			case Candidate:  // 如果是 candidate，则超时是因为出现平票等造成上一任期竞选失败
				// 重置计时器
				// 对于刚竞选失败的 candidate，这个计时是竞选失败等待超时设定
				// 对于已经等待完竞选失败等待超时设定的 candidate，这个计时是选举超时设定
				rf.timer.Stop()
				rf.timer.Reset(time.Duration(getRandTime(300, 500)) * time.Millisecond)

				rf.mu.Lock()
				if rf.electionFlag { // candidate 等待完竞选失败等待超时时间准备好再次参加竞选
					rf.mu.Unlock()
					rf.ConverToCandidate()
					go rf.sendAllRaftRequestVote()
				} else {
					rf.electionFlag = true   // candidate 等这次超时后就可以再次参选
					rf.mu.Unlock()
				}

			case Leader:  // 成为 leader 就不需要超时计时了，直至故障或发现自己的 term 过时
				return
			}
		}
	}
}

/*节点的状态转化为 Follower, 并更新其任期和投票状态。*/
func (rf * Raft) ConverToFollower(term int) {
	rf.serverState = Follower
	rf.currentTerm = term
	rf.votedFor    = noVoted  // 当 term 发生变化时，需要重置 votedFor
}

/*节点的状态转化为 Candidate, 该函数在转换过程中会更新节点的状态，包括角色、任期、投票信息和获得投票个数。
  两种情况下会用到：
  1. Follower 未接到 Leader 的心跳或 Candidate 的投票请求直至超时，会转为 candidate 参加竞选，follower -> candidate
  2. Candidate 由于分票等原因本轮未选出 leader，经过一个超时时间后参加下一轮竞选，candidate -> candidate */
func (rf * Raft) ConverToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.serverState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me

	// DPrintf("Candidate %d run for election! Its current term is %d\n", rf.me, rf.currentTerm)
}

/*节点的状态转化为 Leader*/
func (rf * Raft) ConverToLeader() {
	rf.mu.Lock()
	// DPrintf("Candidate %d was successfully elected as the leader! Its current term is %d\n", rf.me, rf.currentTerm)
	rf.serverState = Leader

	// Leader 状态下，重置 nextindex 与 matchindex 数组，并将它们初始化为当前节点的日志长度。
	rf.nextIndex  = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.logs[len(rf.logs)-1].Index + 1
	}

	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0 // 初始化为 0
	}
	rf.mu.Unlock()

	go func() {
		for !rf.killed() { // leader 没有被 kill 就一直发送
			rf.mu.Lock()
			role := rf.serverState
			rf.mu.Unlock()
			if role == Leader { // 如果 leader 仍然是 leader
				go rf.sendAllRaftAppendEntries()
				time.Sleep(heartBeatInterval) // 心跳间隔
			} else { // 如果当前 server 不再是 leader（变为了 follower）则重启计时器并停止发送心跳包
				rf.timer.Stop()
				rf.timer.Reset(time.Duration(getRandTime(300, 500)) * time.Millisecond)
				go rf.CheckTimeout()
				return
			}
		}
	}()
}

/* Make 创建服务或测试者想要创建的一个 Raft 服务器。所有 Raft 服务器的端口（包括这个）都在 peers[] 数组中。
   这个服务器的端口是 peers[me]。所有服务器的 peers[] 数组顺序相同。persister 是这个服务器保存其持久状态的地方，
   并且最初持有最近保存的状态（如果有的话）。applyCh 是一个通道，测试者或服务期望 Raft 在此通道上发送 ApplyMsg 消息。
   Make() 必须快速返回，因此它应该为任何长期运行的工作启动协程。*/
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:        sync.Mutex{},  // 互斥锁，保护共享访问这个节点的状态
		peers:     peers,         // 所有节点的 RPC 端点
		persister: persister,     // 持久化对象，用于保存这个节点的持久状态
		me:        me,            // 这个节点在 peers[] 数组中的索引
		dead:      0,             // 用于标记节点是否已终止，0 表示还存活

		// Lab2A
		serverState: Follower,    // 初始化节点状态皆为 Follower
		currentTerm: 0,           // 初始化节点当前任期
		votedFor: noVoted,        // 当前任期内接受投票的候选人 id

		// Lab 2B
		commitIndex: 0,
		lastApplied: 0,
		logs:        []LogEntries{{}},
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),   // matchIndex[id] 为最新的已知被复制到跟随者的日志索引
		applyChan:   applyCh,

		timer:       time.NewTimer(time.Duration(getRandTime(300, 500)) * time.Millisecond),
	}

	// 从崩溃前保存的状态进行初始化
	rf.readPersist(persister.ReadRaftState())

	// 起一个 goroutine 循环处理超时
	go rf.CheckTimeout()

	// 起一个 goroutine 循环检查是否有需要应用到状态机日志
	go rf.applyStateMachine()

	return rf
}

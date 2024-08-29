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
	"sync"
	"sync/atomic"
	"time"
	"fmt"
	"math/rand"
//	"6.824/labgob"
	"6.824/labrpc"
)

/*ApplyMsg有两种用途，一种是运送要apply的指令，一种是运送要安装的快照。
  raft server发送ApplyMsg给应用层的channel。在lab2中只需要根据日志
  条目生成msg然后发送到applyCh即认为是日志apply了   */
type ApplyMsg struct {
	CommandValid bool         // 当ApplyMsg用于apply指令时为true，其余时候为false
	Command      interface{}  // 要apply的指令
	CommandIndex int          // 要apply的指令的index

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
	me        int                 // this peer's index into peers[]  当前节点id
	dead      int32               // set by Kill()  是否死亡，1表示死亡，0表示还活着

	// persistent state on all servers
	// 2A
	serverState       RoleType                // 当前节点状态
	currentTerm       int                     // 节点当前任期
	votedFor          int                     // follower把票投给了哪个candidate
	voteCount         int                     // 记录所获选票的个数
	appendEntriesChan chan AppendEntriesReply // 心跳channel
	LeaderMsgChan     chan struct{}           // 当选Leader时发送
	VoteMsgChan       chan struct{}           // 收到选举信号时重置一下计时器，不然会出现覆盖term后计时器超时又突然自增。

	// 2B
	commitIndex       int            // 记录已知被提交的最高日志条目索引（初始化为0，单调递增）
	lastApplied       int            // 记录已应用于状态机的最高日志条目索引（初始化为0，单调递增）
	logs			  [] LogEntries  // 日志条目集合；每个条目包含状态机的命令及该条目被领导者接收时的任期（日志的首个索引为1）
	nextIndex	      []int          // 针对每个服务器，记录下一个将发送给该服务器的日志条目索引（初始化为领导者最后日志索引+1）
	matchIndex        []int          // 针对每个服务器，记录已知在该服务器上复制的最高日志条目索引（初始化为0，单调递增）
	applyChan         chan ApplyMsg  // 通道，用于提交给客户端已经完成超过半数服务器复制成功的log处理结果

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
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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


/*  处理RequestVote RPC请求的处理函数 
任期比较：
	如果请求者的任期大于当前节点的任期，说明请求者的信息更新，当前节点需要更新其任期并转换为Follower角色。
	如果请求者的任期小于当前节点的任期，则当前节点拒绝投票，因为其任期更大，更“新”。*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 锁定当前Raft实例，以保证并发安全。
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 设置返回的任期，
	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		// 如果请求者的任期大于当前节点的任期，则更新当前节点的任期并转换为Follower角色。
		rf.ConverToFollower(args.Term)
		// 重置选票
		rf.votedFor = noVoted
	} else if args.Term < rf.currentTerm { 
		// 如果请求者的任期小于当前节点的任期，或者在同一任期内已经投给了其他候选人，
		// 则直接拒绝投票并返回当前任期与投票结果。
		reply.Term        = rf.currentTerm
		reply.VoteGranted = false
		return
	} else { // 同一任期
		// 在同一任期内已经投给了其他候选人，则直接拒绝投票并返回当前任期与投票结果。
		// 同一任期内不存在未投票的状态。
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			reply.Term        = rf.currentTerm
			reply.VoteGranted = false
			return
		} 
	}

	DPrintf("Server %d gets an RequestVote RPC with a higher term from Candidate %d, and its current term become %d.\n", 
		rf.me, args.CandidateId, rf.currentTerm)

	// 选举投票限制条件：
	// 1-候选人最后一条Log条目的任期号大于本地最后一条Log条目的任期号；
	// 2-或者，候选人最后一条Log条目的任期号等于本地最后一条Log条目的任期号，且候选人的Log记录长度大于等于本地Log记录的长度
	lastLog := rf.logs[len(rf.logs)-1]
	if args.LastLogTerm < lastLog.Term || 
		(args.LastLogTerm == lastLog.Term && args.LastLogIndex < lastLog.Index) {

		DPrintf("Candidate %d fail", args.CandidateId)
		return
	}

	// 如果当前节点是Follower，且未投给其他候选人或已投票给该候选人。
	if rf.serverState == Follower && 
		(rf.votedFor == noVoted || rf.votedFor == args.CandidateId) {
		// 更新投票给的候选人ID。
		rf.votedFor = args.CandidateId
		// 同意投票。
		reply.VoteGranted = true
		reply.Term = args.Term	

		// 向VoteMsgChan通道发送消息，通知其他部分有投票事件发生。
		rf.VoteMsgChan <- struct{}{}
	}

	// // 向VoteMsgChan通道发送消息，通知其他部分有投票事件发生。
	// rf.VoteMsgChan <- struct{}{}
}

// sendRequestVote 发送 RequestVote RPC 给服务器的示例代码。
// server 是目标服务器在 rf.peers[] 中的索引。
// args 中包含 RPC 参数。
// *reply 中填充 RPC 回复，因此调用者应传递 &reply。
// 传递给 Call() 的 args 和 reply 类型必须与处理函数中声明的参数类型相同
// （包括它们是否是指针）。
//
// labrpc 包模拟了一个丢包网络，其中服务器可能无法访问，
// 请求和回复可能会丢失。Call() 发送一个请求并等待回复。
// 如果在超时间隔内收到回复，Call() 返回 true；否则返回 false。
// 因此 Call() 可能不会立即返回。
// 返回 false 可能是由服务器宕机、服务器无法访问、请求丢失或回复丢失引起的。
//
// Call() 保证会返回（可能会有延迟），*除非*服务器端的处理函数没有返回。
// 因此，不需要在 Call() 周围实现你自己的超时机制。
//
// 查看 ../labrpc/labrpc.go 中的注释以了解更多详情。
//
// 如果你在让 RPC 工作时遇到困难，请检查你是否将传递给 RPC 的结构体中的所有字段名都大写，
// 并且调用者传递的是 reply 结构体的地址（&），而不是结构体本身。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	// server下标节点调用RequestVote RPC处理程序
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply); 
	if !ok {
		DPrintf("=>[DEBUG]: server %d call failed serverState %d \n", rf.me, rf.serverState)
		return false
	}

	//并发下加锁保平安
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果该节点已经不是候选者或者该节点请求时的任期与当前任期不一致，直接返回 true，无须继续拉票
	if rf.serverState != Candidate || args.Term != rf.currentTerm {
		return true
	}


	// 如果收到的回复中的任期比当前节点的任期大，遇到了任期比自己大的节点，转换为跟随者 follower
	if reply.Term > rf.currentTerm {
		rf.ConverToFollower(reply.Term)
		// // 向VoteMsgChan通道发送消息，通知其他部分有投票事件发生。
		// rf.VoteMsgChan <- struct{}{}
		return true
	}


	// 如果收到投票，且当前节点仍是候选者，增加投票计数
	if reply.VoteGranted && rf.serverState == Candidate {
		rf.voteCount++;

		DPrintf("Candidate %d got a vote from server %d, current voteCount %d!\n", rf.me, server, rf.voteCount)

		// 如果当前节点的投票数超过一半，且节点仍为候选者，转为Leader
		if 2*rf.voteCount > len(rf.peers) && rf.serverState == Candidate {
			rf.ConverToLeader()
			// 超半数票 直接当选，当选为领导者后，通知 LeaderMsgChan
			rf.LeaderMsgChan <- struct{}{}
		} else { // 收到所有回复但选票仍不够的情况，即竞选失败
			DPrintf("Candidate %d failed in the election and continued to wait...\n", rf.me)
		}
	}

	return true
}

/* 向其他Raft节点并行发送投票请求的RPC。
*/
func (rf *Raft) sendAllRaftRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLog := rf.logs[len(rf.logs)-1]

	// 构建请求投票的参数
	args := &RequestVoteArgs {
		Term: 			rf.currentTerm, // 当前任期
		CandidateId: 	rf.me,          // 候选人 ID
		LastLogIndex:   lastLog.Index,  // 候选人最后一个日志条目的索引（暂时设置为 0）
		LastLogTerm:    lastLog.Term,   // 候选人最后一个日志条目的任期（暂时设置为 0）
	}

	// 向所有其他节点发送请求投票的 RPC
	for index := range rf.peers {

		if rf.killed() {
			DPrintf("Candidate %d is dead !\n", rf.me)
			return 
		}

		// 向除当前节点外的其他节点发送RPC，且当前节点为候选者状态
		if index != rf.me && rf.serverState == Candidate {
			// 并行发送请求投票的RPC
			go func(id int) {
				// 构建返回参数
				ret := &RequestVoteReply {
					Term:        0,
					VoteGranted: false,
				}
				ok := rf.sendRequestVote(id, args, ret)
				if !ok {
					DPrintf("Candidate %d call server %d for RequestVote failed!\n", rf.me, id)
				}
			}(index)
		}
	}

}

/*处理来者Leader的AppendEntries的RPC请求， 处理接收到的 AppendEntries 请求，包括心跳和日志条目的复制*/
func (rf * Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server %d gets an AppendEntries RPC(term:%d, Entries len:%d) with a higher term from Leader %d, and its current term become %d.\n",
		rf.me, args.Term, len(args.Entries), args.LeaderId, rf.currentTerm)

	// 初始化响应的任期为当前任期
	reply.Term = rf.currentTerm

	/*============2A-选举============*/
	// 当前请求的Term小于当前节点的Term，那么说明收到了来自过期的领导人的附加日志请求，那么拒接处理。
	if rf.currentTerm > args.Term {
		return 
	}

	// 收到Leader更高的任期时，更新自己的任期，转为 leader 的追随者 
	// 或者当前节点是candidate，且任期等于leader任期（表明节点中存在多个Candidate），转为候选者。
	if rf.currentTerm < args.Term || (rf.currentTerm == args.Term && rf.serverState == Candidate){
		rf.ConverToFollower(args.Term)
	}

	// 发送心跳或日志条目后
	rf.appendEntriesChan <- AppendEntriesReply{Term: rf.currentTerm, Success: true}

	// DPrintf("Server %d gets an AppendEntries RPC(term:%d, Entries len:%d) with a higher term from Leader %d, and its current term become %d.\n",
	// 	rf.me, args.Term, len(args.Entries), args.LeaderId, rf.currentTerm)

	/*============2B-新增日志复制功能============*/
	// 如果preLogIndex的长度大于当前的日志的长度，那么说明跟随者缺失日志。
	// 情况一：leader请求的PrevLogIndex在follower中不存在，大于follower的日志长度。
	if args.PrevLogIndex >= len(rf.logs) {
		/* 领导者和跟随者之间的日志出现了不一致，或者跟随者落后很多且领导者的信息过时。领导者收到这样的失败响应后，会根据跟随者的反馈调整其nextIndex值，
		然后重试发送AppendEntries请求，从一个更早的索引开始，以解决日志不一致的问题。这样，通过一系列的尝试与调整，Raft算法能够最终确保集群间日志的一致性。
		*/
		reply.ConflictIndex = rf.logs[len(rf.logs)-1].Index + 1
		reply.ConflictTerm  = -1
		return
	}

	// 优化逻辑：处理日志条目任期不匹配的情况, 那么说明跟随者的日志不缺失。
	// 情况二：leader请求的PrevLogIndex在follower中存在，但是follower中PrevLogIndex处的Term在与Leader的PrevLogTerm不同。
	if args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		// Follower处存在PrevLogIndex，记录follower中PreLogIndex位置处任期号。
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
		// 然后在follower的日志中搜索任期等于 conflictTerm 的第一个条目索引。
		for index := args.PrevLogIndex; index >= 0; index-- {
			// 遍历寻找Follower的任期与PrevLogIndex处任期不一致的条目时，记录其下一个条目的索引作为冲突索引。
			if rf.logs[index].Term != rf.logs[args.PrevLogIndex].Term {
				reply.ConflictIndex = index + 1
				break
			}
		}
		// 收集请求的冲突信息，返回。
		return 
	}

	// 如果请求的PrevLogIndex与Term在follower中存在，表明匹配到了两个日志一致的最新日志条目
	reply.Success = true

	// 情况三：leader请求的PrevLogIndex在follower中存在，且follower中PrevLogIndex处的Term在与Leader的PrevLogTerm相同。
	// 分析：3.1-表明leader的日志长度可能与follower的日志长度相等。3.2-表明leader的日志长度可能大于follower的日志长度。
	// 针对情况三需要考虑follower日志在PrevLogIndex之后的日志条目与leader发送的Entries是否index与term相同。
	// 分为两种：相同与不相同的情况。
	for index := 0; index < len(args.Entries); index++ {
		// 计算leader请求的Entries中当前条目索引在follower日志中的目标索引位置
		currentIdx := index + args.PrevLogIndex + 1 // Entries中条目在follower日志条目的索引偏移量
		// 1-针对3.1;判断当前条目索引是否超出follower日志的长度，若超出直接在follower日志的尾部currentIdex后添加leader发送的entries
		// 2-针对3.2;判断当前条目的任期是否与follower在currentIdx处日志的任期相同，若不同，表明index相同，term不同，表明leader
		// 请求的添加的日志在follower中已经存在日志中冲突。
		if currentIdx >= len(rf.logs) || rf.logs[currentIdx].Term != args.Entries[index].Term {
			// 如果存在落后或者冲突，以当前领导者的日志为主，从当前位置开始，用领导者发送的日志条目替换或追加到跟随者日志中
			// 这里使用切片操作，保留原有日志的前半部分，然后追加领导者发送的日志条目
			rf.logs = append(rf.logs[:currentIdx], args.Entries[index:]...)
			// 替换或追加完毕后，跳出循环
			break
		}
	}	

	// 更新commitIndex
	// 保持一致性：args.LeaderCommit是领导者告知的已提交日志的最高索引。跟随者需要确保自己的commitIndex至少达到这个值，
	// 以保证整个集群的一致性。如果跟随者的日志足够新，能够包含领导者所提交的所有日志条目，
	// 那么跟随者的commitIndex就应该更新为args.LeaderCommit，以反映集群的最新一致状态。

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logs[len(rf.logs)-1].Index)
	}

	fmt.Printf("server %d follower logs len : %d \n", rf.me, len(rf.logs))

}

/*向指定的节点发送 AppendEntries RPC 请求, 并处理响应。*/
func (rf * Raft) sendAppendEntries(id int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[id].Call("Raft.AppendEntriesHandler", args, reply)
	if !ok {
		// 发送失败直接返回即可。
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 2A-选举
    // 如果当前节点不再是领导者，则直接返回
    if rf.serverState != Leader {
       return false
    }

    // 如果响应中的任期大于当前任期，当前节点会转换为跟随者
    if reply.Term > rf.currentTerm {
       rf.ConverToFollower(reply.Term)
    }

	// 2B-新增日志复制功能
	// 阻止过时RPC：leader 发送的请求的任期与当前任期不同，则直接返回
	if args.Term != rf.currentTerm {
		return false
	}

	// 当接收的AppendEntries RPC响应失败，意味着跟随者上的日志与领导者尝试追加的日志条目之间存在不一致。
	// leader通过重复发送AppendEntries的方式解决：
	// 情况1-响应的冲突任期为-1： 表明leader发送的append信息preLogIndex的长度大于当前的日志的长度，那么说明跟随者缺失日志
	// 解决：调整leader的nextindx的日志条目索引。nextindex = ConflictIndex
	// 情况2-响应的冲突任期不为-1： 表明leader与follower的日志条目存在任期不匹配的情况, 且跟随者的日志不缺失。
	// 解决：搜索Leader日志中任期为ConfictTerm的条目，若存在，修改nextindex为其日志中此任期的最后一个条目的索引+1。
	// 若不存在，修改nextindex为响应的ConflictIndex,重置follower中此任期的所有日志信息。
	if !reply.Success {
		if reply.ConflictIndex == -1 {
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
				// 如果没有找到冲突任期，则设置nextIndex为冲突索引
				rf.nextIndex[id] = reply.ConflictIndex
			}
		}
	} else {
		// 同步成功，根据guide，你不能假设server的状态在它发送RPC和收到回复之间没有变化。
		// 因为可能在这期间收到新的指令而改变了log和nextIndex
		possibleMatchIdx := args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[id]  = max(possibleMatchIdx + 1, rf.nextIndex[id])

		// 更新matchIndex[id]为最新的已知被复制到跟随者的日志索引
		rf.matchIndex[id] = max(possibleMatchIdx, rf.matchIndex[id])

		// // 根据Figure2 Leader Rule 4，确定满足commitIndex < N <= 大多数matchIndex[i]且在当前任期的N
		// sortMatchIndex := make([]int, len(rf.peers))
		// copy(sortMatchIndex, rf.matchIndex)
		// sort.Ints(sortMatchIndex)                         // 升序排序
		// maxN := sortMatchIndex[(len(sortMatchIndex)-1)/2] // 满足N <= 大多数matchIndex[i] 的最大的可能的N
		// for N := maxN; N > rf.commitIndex; N-- {
		// 	if rf.logs[N].Term == rf.currentTerm {
		// 		rf.commitIndex = N // 如果log[N]的任期等于当前任期则更新commitIndex
		// 		// DPrintf("Leader%d's commitIndex is updated to %d.\n", rf.me, N)
		// 		break
		// 	}
		// }
	}

	fmt.Printf("leader copy to follower : %d \n",rf.matchIndex)

	return true
}

/*领导者向其他所有节点发送附加日志条目（或心跳）请求。在领导者周期性地发送心跳或需要复制日志条目到所有节点时使用*/
func (rf *Raft) sendAllRaftAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for server := range rf.peers {

		if rf.killed() {
			return
		}

		// 对于每个不是当前节点的节点，leader 启动一个新的 goroutine 来发送 AppendEntries 请求
		if server != rf.me && rf.serverState == Leader {

			nextId  := rf.nextIndex[server] // 获取Leader对于第id个服务器的下一个要发送的日志id
			lastLog := rf.logs[nextId-1]    // 获取上次发送给follower的最后一条日志条目
			var logs []LogEntries

			// 如果leader的日志从nextIdx开始有要发送的日志，则此AppendEntries RPC需要携带从nextIdx开始的日志条目
			if rf.logs[len(rf.logs)-1].Index >= nextId {
				logs = make([]LogEntries, len(rf.logs)-nextId) // 创建一个新切片，用于存放需要同步给follower的尚未同步的日志条目
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

			go func(id int) {
				reply := &AppendEntriesReply {
					Term: 0,
					Success: false,
				}
				DPrintf("Leader %d sends AppendEntries RPC(term:%d, Entries len:%d, logs len:%d, nextId:%d | preIndex:%d, PreTerm:%d) to server %d...\n", 
					rf.me, rf.currentTerm, len(args.Entries), len(rf.logs), nextId, args.PrevLogIndex, args.PrevLogTerm, id)
				ok := rf.sendAppendEntries(id, args, reply)
				if !ok {
					// 如果由于网络原因或者follower故障等收不到RPC回复（不是follower将回复设为false）
					// 则leader无限期重复发送同样的RPC（nextIndex不前移），等到下次心跳时间到了后再发送
					DPrintf("Leader %d calls server %d for AppendEntries or Heartbeat failed!\n", rf.me, id)
					return
				}
			}(server)
		}
	}
}

/*领导者节点根据Raft算法的规则检查并更新commitIndex。*/
func (rf *Raft) checkCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 从最后一条日志开始遍历，倒序遍历日志条目，快照保存的日志不需要检查
	for idx := len(rf.logs) - 1; idx >= rf.commitIndex; idx-- {
		// figure8简化版本
		// Leader 不能直接提交不属于Leader任期的日志条目
		if rf.logs[idx].Term < rf.currentTerm || rf.serverState != Leader {
			return
		}
		count := 1
		// 遍历每个服务器的matchIndex，如果大于等于当前遍历的日志索引，则表明该服务器已经成功复制日志，计数加1
		for i := range rf.matchIndex {
			// 需要加上快照的最后一条索引
			if i != rf.me && rf.matchIndex[i] >= idx {
				count++
			}
		}
		// 如果计数大于半数，则更新commitIndex为当前遍历的日志索引，允许更多的日志条目被状态机应用。
		if count > len(rf.peers)/2 {
			if rf.serverState == Leader {
				// 在leader的日志索引idx下，leader的日志成功附加到集群中的N/2+1的节点上，更新leader的commitIndex
				rf.commitIndex = idx
				DPrintf("Leader%d's commitIndex is updated to %d.\n", rf.me, idx)
			}
			break
		}
	}
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	// 检查是否有新的日志需要应用
	// 如果commitIndex不大于lastApplied，说明没有新的日志需要应用
	if rf.commitIndex <= rf.lastApplied {
		rf.mu.Unlock()
		return
	} 

	// 创建一个新的日志条目切片，用于存储需要应用的日志，应用后就可以追上已提交的日志
	copyLogs := make([]LogEntries, rf.commitIndex-rf.lastApplied)
	// 复制需要应用的日志条目到copyLogs中
	// rf.lastApplied+1 是通过已经上次应用的日志索引获取这次需要应用的日志的起始索引
	copy(copyLogs, rf.logs[rf.lastApplied+1:rf.commitIndex+1])
	// 应用日志条目之后更新lastApplied（最新已应用的日志索引）
	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()

	// 这里不要加锁 2D测试函数会死锁
	// 遍历从lastApplied+1到commitIndex的所有日志条目
	for _, logEntity := range copyLogs {
		// 将每个条目的命令通过applyChan发送出去，以供状态机执行
		rf.applyChan <- ApplyMsg{
			CommandValid: true,              // 包含一个新提交的日志条目
			Command:      logEntity.Command, // 新提交的日志条目
			CommandIndex: logEntity.Index,   // 新提交日志条目的索引
		}
	}
}

// apply 方法负责将已知提交的日志条目应用到状态机中。
// 这个方法检查是否有新的日志条目可以被应用，并将它们发送到applyChan，
// 以便状态机能够处理这些条目并更新其状态。
func (rf *Raft) applyStateMachine() {
	for !rf.killed() {   // 如果server没有被kill就一直检测
		// 确保已提交的日志条目被及时应用到状态机上
		rf.apply()
		time.Sleep(commitInterval)
	}
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.serverState != Leader {
		DPrintf("Client sends a new commad to Server %d but lt's not Leader!\n", rf.me)
		return -1, -1, false 
	}

	addLog := LogEntries {
		Command: command,
		Term:    rf.currentTerm,
		Index:   rf.logs[len(rf.logs)-1].Index + 1,
	}

	// 增加新条目到日志列表中
	rf.logs = append(rf.logs, addLog)
	index = addLog.Index
	term  = addLog.Term

	DPrintf("[Start]Client sends a new commad to Leader %d!\n", rf.me)

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
func (rf *Raft) ticker() {

	for !rf.killed() {
		// 在这里添加代码以检查是否应该启动领导者选举
		// 并使用 time.Sleep() 随机化休眠时间。
		rf.mu.Lock()
		role := rf.serverState
		rf.mu.Unlock()
		switch role {
		case Candidate:
			select {
			case <-rf.VoteMsgChan:  // TODO  此状态存疑?
				// continue
			case resp := <-rf.appendEntriesChan:
				if resp.Term >= rf.currentTerm {
				   // 关键点：候选者收到更大任期的leader的心跳信息或者日志复制信息后，需要转为follower
				   rf.ConverToFollower(resp.Term)
				   continue
				}
			case <-time.After(electionTimeout + time.Duration(rand.Int31()%300)*time.Millisecond):
				// 选举超时 重置选举状态。 electionTimeout为：选举超时时间, 
				// time.Duration(rand.Int31()%300)*time.Millisecond为：竟选失败等待时间
				
				if rf.serverState == Candidate {
					rf.ConverToCandidate()
					go rf.sendAllRaftRequestVote()
					// continue	
				}
			case <-rf.LeaderMsgChan:
			}

		case Leader:
			// Leader 定期发送心跳和同步日志
			rf.sendAllRaftAppendEntries()

			// 更新commitIndex对子节点中超过半数复制的日志进行提交
			go rf.checkCommitIndex()

			select {
			case resp := <-rf.appendEntriesChan:
				// 处理跟随者的响应，如发现更高的任期则转为Follower
				if resp.Term > rf.currentTerm {
					rf.ConverToFollower(resp.Term)
					continue
				}
			case <-time.After(heartBeatInterval):
				// 超时后继续发送心跳
				continue
			}

		case Follower:
			select {
			case <-rf.VoteMsgChan: 
				// 收到投票消息，表明投过票，维持Follower状态，继续等待。
				continue
			case resp := <-rf.appendEntriesChan:
				// 收到附加日志条目消息，继续等待。
				if resp.Term > rf.currentTerm {
					rf.ConverToFollower(resp.Term)
					continue
				}
			case <-time.After(appendEntriesTimeout + time.Duration(rand.Int31()%300)*time.Millisecond):
				// (心跳超时，例如Leader失效) 附加日志条目超时，转换为候选人，发起选举。增加扰动避免多个Candidate同时进入选举
				// appendEntriesTimeout为：心跳超时时间（距离上一次心跳时间间隔）
				// time.Duration(rand.Int31()%300)*time.Millisecond：为follower的超时参选时间
				rf.ConverToCandidate()
				go rf.sendAllRaftRequestVote()
			}
		}

	}
}

/*节点的状态转化为Follower, 并更新其任期和投票状态。*/
func (rf * Raft) ConverToFollower(term int) {
	rf.serverState = Follower
	rf.currentTerm = term
}

/*节点的状态转化为Candidate, 该函数在转换过程中会更新节点的状态，包括角色、任期、投票信息和获得投票个数。*/
func (rf * Raft) ConverToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.serverState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount= 1

	DPrintf("Candidate %d run for election! Its current term is %d\n", rf.me, rf.currentTerm)
}

/*节点的状态转化为Leader*/
func (rf * Raft) ConverToLeader() {

	DPrintf("Candidate %d was successfully elected as the leader! Its current term is %d\n", rf.me, rf.currentTerm)
	rf.serverState = Leader

	// Leader状态下，重置nextindex与matchindex数组，并将它们初始化为当前节点的日志长度。
	rf.nextIndex  = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.logs[len(rf.logs)-1].Index + 1
	}

	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0 // 初始化为0
	}
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
		dead:      0,             // 用于标记节点是否已终止, 0表示还存活

		// Lab2A
		serverState: Follower,    // 初始化节点状态皆为Follower
		currentTerm: 0,           // 初始化节点当前任期
		votedFor: noVoted,        // 当前任期内接受投票的候选人id
		voteCount: 0,
		appendEntriesChan: make(chan AppendEntriesReply), // 用于心跳信号的通道
		LeaderMsgChan: make(chan struct{}, chanLen),      // 用于领导者选举信号的通道
		VoteMsgChan: make(chan struct{}, chanLen),        // 用于投票信号的通道

		// Lab 2B
		commitIndex: 0,
		lastApplied: 0,
		logs:        []LogEntries{{}},
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),   // matchIndex[id]为最新的已知被复制到跟随者的日志索引
		applyChan:   applyCh,
	}
	
	// 从崩溃前保存的状态进行初始化
	rf.readPersist(persister.ReadRaftState())

	// 启动 ticker 协程开始选举
	go rf.ticker()

	// 起goroutine循环检查是否有需要应用到状态机日志
	go rf.applyStateMachine()

	return rf
}

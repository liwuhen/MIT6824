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
	// "fmt"
	"math/rand"
//	"6.824/labgob"
	"6.824/labrpc"
)


type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

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
	serverState       RoleType                // 当前节点状态
	currentTerm       int                     // 节点当前任期
	votedFor          int                     // follower把票投给了哪个candidate
    voteCount         int                     // 记录所获选票的个数
    appendEntriesChan chan AppendEntriesReply // 心跳channel
    LeaderMsgChan     chan struct{}           // 当选Leader时发送
    VoteMsgChan       chan struct{}           // 收到选举信号时重置一下计时器，不然会出现覆盖term后计时器超时又突然自增。

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
		// reply.Term        = rf.currentTerm
		// reply.VoteGranted = false
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
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
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
	}


	// 如果收到投票，且当前节点仍是候选者，增加投票计数
	if reply.VoteGranted && rf.serverState == Candidate {
		rf.voteCount++;
		// 如果当前节点的投票数超过一半，且节点仍为候选者，转为Leader
		if 2*rf.voteCount > len(rf.peers) && rf.serverState == Candidate {
			rf.ConverToLeader()
		}
	}

	return true
}

/* 向其他Raft节点并行发送投票请求的RPC。
*/
func (rf *Raft) sendAllRaftRequestVote() {
	rf.mu.Lock()
    defer rf.mu.Unlock()

	// 构建请求投票的参数
	args := &RequestVoteArgs {
		Term: 			rf.currentTerm, // 当前任期
		CandidateId: 	rf.me,          // 候选人 ID
		LastLogIndex:   0,              // 候选人最后一个日志条目的索引（暂时设置为 0）
		LastLogTerm:    0,              // 候选人最后一个日志条目的任期（暂时设置为 0）
	}

	// 向所有其他节点发送请求投票的 RPC
	for index := range rf.peers {
		// 向除当前节点外的其他节点发送RPC，且当前节点为候选者状态
		if index != rf.me && rf.serverState == Candidate {
			// 并行发送请求投票的RPC
			go func(id int) {
				// 构建返回参数
				ret := &RequestVoteReply {
					Term:        0,
					VoteGranted: false,
				}
				rf.sendRequestVote(id, args, ret)
			}(index)
		}
	}

}

/*处理来者Leader的AppendEntries的RPC请求， 处理接收到的 AppendEntries 请求，包括心跳和日志条目的复制*/
func (rf * Raft) AppendEntriesHandler(id int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
    defer rf.mu.Unlock()

	// 传一个空结构体表示接收到了Leader的请求。
	// 初始化响应的任期为当前任期
	reply.Term = rf.currentTerm

	// 收到Leader更高的任期时，更新自己的任期，转为 leader 的追随者
	if rf.currentTerm < args.Term {
		rf.ConverToFollower(args.Term)
		return
	}

	// 向 appendEntriesChan 发送一个空结构体，以表示接收到了领导者的请求
	//rf.appendEntriesChan <- struct {}{}
	// 发送心跳或日志条目后
	rf.appendEntriesChan <- AppendEntriesReply{Term: rf.currentTerm, Success: true}
}

/*向指定的节点发送 AppendEntries RPC 请求, 并处理响应。*/
func (rf * Raft) sendAppendEntries(id int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.peers[id].Call("Raft.AppendEntriesHandler", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

    // 如果当前节点不再是领导者，则直接返回
    if rf.serverState != Leader {
       return
    }

    // 如果响应中的任期大于当前任期，当前节点会转换为跟随者
    if reply.Term > rf.currentTerm {
       rf.ConverToFollower(reply.Term)
       return
    }
}

/*领导者向其他所有节点发送附加日志条目（或心跳）请求。在领导者周期性地发送心跳或需要复制日志条目到所有节点时使用*/
func (rf *Raft) sendAllRaftAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for server := range rf.peers {
		// 对于每个不是当前节点的节点，leader 启动一个新的 goroutine 来发送 AppendEntries 请求
		if server != rf.me && rf.serverState == Leader {
			go func(id int) {
				args := &AppendEntriesArgs {
					Term: rf.currentTerm,
					LeaderId: rf.me,
					PrevLogIndex: 0,
					PrevLogTerm: 0,
					LeaderCommit: 0,
				}
				reply := &AppendEntriesReply {
					Term: 0,
					Success: false,
				}
				rf.sendAppendEntries(id, args, reply)
			}(server)
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	for rf.killed() == false {

		// 在这里添加代码以检查是否应该启动领导者选举
        // 并使用 time.Sleep() 随机化休眠时间。
		rf.mu.Lock()
		role := rf.serverState
		rf.mu.Unlock()
		switch role {
		case Candidate:
			go rf.sendAllRaftRequestVote()
			select {
			case <-rf.VoteMsgChan:  // TODO  此状态存疑?
				continue
			case resp := <-rf.appendEntriesChan:
				if resp.Term >= rf.currentTerm {
				   // 关键点：候选者收到更大任期的leader的心跳信息或者日志复制信息后，需要转为follower
				   rf.ConverToFollower(resp.Term)
				   continue
				}
			case <-time.After(electionTimeout + time.Duration(rand.Int31()%300)*time.Millisecond):
				// 选举超时 重置选举状态。 electionTimeout为：选举超时时间, 
				// time.Duration(rand.Int31()%300)*time.Millisecond为：竟选失败等待时间
				rf.ConverToCandidate()
				continue
			case <-rf.LeaderMsgChan:

			}	

		case Leader:
			// Leader 定期发送心跳和同步日志
			rf.sendAllRaftAppendEntries()
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
			case <- rf.VoteMsgChan: 
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
			}
		}

	}
}

/*节点的状态转化为Follower, 并更新其任期和投票状态。*/
func (rf * Raft) ConverToFollower(term int) {
	rf.serverState = Follower
	rf.currentTerm = term
}

/*节点的状态转化为Leader*/
func (rf * Raft) ConverToLeader() {
	rf.serverState = Leader
}

/*节点的状态转化为Candidate, 该函数在转换过程中会更新节点的状态，包括角色、任期、投票信息和获得投票个数。*/
func (rf * Raft) ConverToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.serverState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount= 1
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
	}
	
	// 从崩溃前保存的状态进行初始化
	rf.readPersist(persister.ReadRaftState())

	// 启动 ticker 协程开始选举
	go rf.ticker()

	return rf
}

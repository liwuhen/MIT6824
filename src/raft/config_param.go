package raft
import "time"
import "math/rand"

const noVoted = -1  // noVoted 表示没有投票，常量值为 -1

// commitInterval rf 节点提交日志的间隔时间
var commitInterval = 10 * time.Millisecond

// heartBeatInterval 心跳间隔 每 1 秒发送 10 次 领导者发送心跳 RPC 的频率不超过每秒 10 次
var heartBeatInterval = 100 * time.Millisecond

type RoleType int // 节点的三种状态

const (
	Leader RoleType = iota // 0
	Candidate              // 1
	Follower			   // 2
)

func min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

// getFirstLog 获取第一个日志
func (rf *Raft) getFirstLog() LogEntries {
	return rf.logs[0]
}

// 获取 l~r 毫秒范围内一个随机毫秒数
func getRandTime(l int, r int) int {
	// 如果每次调 rand.Intn() 前都调了 rand.Seed(x)，每次的 x 相同的话，每次的 rand.Intn() 也是一样的（伪随机）
	// 推荐做法：只调一次 rand.Seed()：在全局初始化调用一次 seed，每次调 rand.Intn() 前都不再调 rand.Seed()。
	// 此处采用使 Seed 中的 x 每次都不同来生成不同的随机数，x 采用当前的时间戳
	rand.Seed(time.Now().UnixNano())
	ms := l + (rand.Intn(r - l)) // 生成 l~r 之间的随机数（毫秒）
	return ms
}
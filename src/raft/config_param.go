package raft
import "time"

var chanLen = 10  // chan 缓冲区长度

const noVoted = -1  // noVoted 表示没有投票，常量值为 -1

// commitInterval rf 节点提交日志的间隔时间
var commitInterval = 200 * time.Millisecond

// 选举超时时间为 1s
// 由于测试程序限制心跳频率为每秒 10 次，选举超时需要比论文的 150 到 300 毫秒更长，
// 但不能太长，以避免在五秒内未能选出领导者。
var electionTimeout = 300 * time.Millisecond

// heartBeatInterval 心跳间隔 每 1 秒发送 10 次 领导者发送心跳 RPC 的频率不超过每秒 10 次
var heartBeatInterval = 100 * time.Millisecond

var appendEntriesTimeout = 300 * time.Millisecond // 心跳超时时间为 200ms

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
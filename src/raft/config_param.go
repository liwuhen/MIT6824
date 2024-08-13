package raft
import "time"

var chanLen = 10  // chan缓冲区长度

const noVoted = -1  // noVoted 表示没有投票，常量值为-1

// 选举超时时间为 1s
// 由于测试程序限制心跳频率为每秒10次，选举超时需要比论文的150到300毫秒更长，
// 但不能太长，以避免在五秒内未能选出领导者。
var electionTimeout = 300 * time.Millisecond

// heartBeatInterval 心跳间隔 每1秒发送10次 领导者发送心跳RPC的频率不超过每秒10次
var heartBeatInterval = 100 * time.Millisecond

var appendEntriesTimeout = 300 * time.Millisecond // 心跳超时时间为200ms

type RoleType int // 节点的三种状态

const (
	Leader RoleType = iota // 0
	Candidate              // 1
	Follower			   // 2
)
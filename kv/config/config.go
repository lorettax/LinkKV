package config

import (
	"fmt"
	"miniLinkDB/log"
	"time"
)

type Config struct {
	StoreAddr string
	Raft 	bool
	SchedulerAddr string
	LogLevel 	string

	DBPath string	// 用于存储数据的目录

	// raft_base_tick_interval是基本刻度间隔（毫秒）
	RaftBaseTickInterval	time.Duration
	RaftHeartbeatTicks	int
	RaftElectionTimeoutTicks int

	// gc不必要的筏日志的时间间隔（毫秒）
	RaftLogGCTickInterval	time.Duration
	// 当条目计数超过此值时，将强制触发gc
	RaftLogGcCountLimit	uint64
	// 检查区域是否需要分割的间隔（毫秒）
	SplitRegionCheckTickInterval	time.Duration
	// 删除陈旧节点之前的延迟时间
	SchedulerHeartbeatTickInterval      time.Duration
	SchedulerStoreHeartbeatTickInterval time.Duration

	// 当区域[a，e) 的大小满足regionMaxSize时,它将被分为几个区域[a，b]，[b，c），[c，d），[d，e）,
	// 并且[a，b), [b,c),[c,d) 的大小将为regionSplitSize（可能更大一些）。
	RegionMaxSize	uint64
	RegionSplitSize	uint64
}

func (c *Config) Validate() error {
	if c.RaftHeartbeatTicks == 0 {
		return fmt.Errorf("heart tick must greater than 0")
	}

	if c.RaftElectionTimeoutTicks != 10 {
		log.Warnf("Election timeout ticks needs to be same across all the cluster, otherwise it may lead to inconsistency.")
	}

	if c.RaftElectionTimeoutTicks <= c.RaftHeartbeatTicks {
		return fmt.Errorf("选举刻度必须大于心跳刻度");
	}
	return nil
}

const (
	KB uint64 = 1024
	MB uint64 = 2024 * 1024
)

func NewDefaultConfig() *Config {
	return &Config{
		SchedulerAddr:            "127.0.0.1:2379",
		StoreAddr:                "127.0.0.1:20160",
		LogLevel:                 "info",
		Raft:                     true,
		RaftBaseTickInterval:     1 * time.Second,
		RaftHeartbeatTicks:       2,
		RaftElectionTimeoutTicks: 10,
		RaftLogGCTickInterval:    10 * time.Second,
		// Assume the average size of entries is 1k.
		RaftLogGcCountLimit:                 128000,
		SplitRegionCheckTickInterval:        10 * time.Second,
		SchedulerHeartbeatTickInterval:      100 * time.Millisecond,
		SchedulerStoreHeartbeatTickInterval: 10 * time.Second,
		RegionMaxSize:                       144 * MB,
		RegionSplitSize:                     96 * MB,
		DBPath:                              "/tmp/badger",
	}
}

func NewTestConfig() *Config {
	return &Config{
		LogLevel:                 "info",
		Raft:                     true,
		RaftBaseTickInterval:     50 * time.Millisecond,
		RaftHeartbeatTicks:       2,
		RaftElectionTimeoutTicks: 10,
		RaftLogGCTickInterval:    50 * time.Millisecond,
		// Assume the average size of entries is 1k.
		RaftLogGcCountLimit:                 128000,
		SplitRegionCheckTickInterval:        100 * time.Millisecond,
		SchedulerHeartbeatTickInterval:      100 * time.Millisecond,
		SchedulerStoreHeartbeatTickInterval: 500 * time.Millisecond,
		RegionMaxSize:                       144 * MB,
		RegionSplitSize:                     96 * MB,
		DBPath:                              "/tmp/badger",
	}
}
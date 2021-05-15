package message

import (
	"github.com/Connor1996/badger"
	"miniLinkDB/proto/pkg/raft_cmdpb"
	"time"
)

type Callback struct {
	Resp 	*raft_cmdpb.RaftCmdResponse
	Txn 	*badger.Txn
	done 	chan struct{}
}

func (cb *Callback) Done(resp *raft_cmdpb.RaftCmdResponse) {
	if cb == nil {
		return
	}
	if resp != nil {
		cb.Resp = resp
	}
	cb.done <- struct{}{}
}

func (cb *Callback) WaitResp() *raft_cmdpb.RaftCmdResponse {
	select {
	case <-cb.done:
		return cb.Resp
	}
}

func (cb *Callback) WaitRespWithTimeout(timeout time.Duration) *raft_cmdpb.RaftCmdResponse {
	select {
	case <-cb.done:
		return cb.Resp
	case <-time.After(timeout):
		return cb.Resp
	}
}

func NewCallback() *Callback {
	done := make(chan struct{}, 1)
	cb := &Callback{done: done}
	return cb
}

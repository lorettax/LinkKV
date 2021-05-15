package raft_storage

import (
	"context"
	"miniLinkDB/errors"
	"miniLinkDB/proto/pkg/errorpb"
	"miniLinkDB/proto/pkg/kvrpcpb"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"miniLinkDB/kv/config"
	"miniLinkDB/kv/raftstore"
	"miniLinkDB/kv/raftstore/message"
	"miniLinkDB/kv/raftstore/scheduler_client"
	"miniLinkDB/kv/raftstore/snap"
	"miniLinkDB/kv/util/worker"

	"miniLinkDB/kv/storage"
	"miniLinkDB/kv/util/engine_util"

	"miniLinkDB/proto/pkg/linkkvpb"
	"miniLinkDB/proto/pkg/raft_cmdpb"
)
// RaftStorage是由Raft节点支持的“ Storage”参见 linkkvserver.go的实现。它是Raft网络的一部分.
// By using Raft,读取和写入与LinkKV实例中的其他节点一致.
type RaftStorage struct {
	engines *engine_util.Engines
	config	*config.Config

	node	*raftstore.Node
	snapManager *snap.SnapManager
	raftRouter *raftstore.RaftstoreRouter
	raftSystem	*raftstore.Raftstore
	resolveWorker	*worker.Worker
	snapWorker	*worker.Worker

	wg	sync.WaitGroup
}

type RegionError struct {
	RequestErr *errorpb.Error
}

func (re *RegionError) Error() string {
	return re.RequestErr.String()
}

func (rs *RaftStorage) checkResponse(resp *raft_cmdpb.RaftCmdResponse, reqCount int) error {
	if resp.Header.Error != nil {
		return &RegionError{RequestErr: resp.Header.Error}
	}
	if len(resp.Responses) != reqCount {
		return errors.Errorf("responses count %d is not equal to requests count %d",
			len(resp.Responses), reqCount)
	}
	return nil
}

// NewRaftStorage 创建一个由raftstore支持的新存储引擎。
func NewRaftStorage(conf *config.Config) *RaftStorage {
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")
	snapPath := filepath.Join(dbPath, "snap")

	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)
	os.Mkdir(snapPath, os.ModePerm)

	raftDB := engine_util.CreateDB(raftPath, true)
	kvDB := engine_util.CreateDB(kvPath, false)
	engines := engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)

	return &RaftStorage{engines: engines, config: conf}
}

func (rs *RaftStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	var reqs []*raft_cmdpb.Request
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			reqs = append(reqs, &raft_cmdpb.Request{
				CmdType: raft_cmdpb.CmdType_Put,
				Put: &raft_cmdpb.PutRequest{
					Cf:    put.Cf,
					Key:   put.Key,
					Value: put.Value,
				}})
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			reqs = append(reqs, &raft_cmdpb.Request{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete: &raft_cmdpb.DeleteRequest{
					Cf:  delete.Cf,
					Key: delete.Key,
				}})
		}
	}

	header := &raft_cmdpb.RaftRequestHeader{
		RegionId:    ctx.RegionId,
		Peer:        ctx.Peer,
		RegionEpoch: ctx.RegionEpoch,
		Term:        ctx.Term,
	}
	request := &raft_cmdpb.RaftCmdRequest{
		Header:   header,
		Requests: reqs,
	}
	cb := message.NewCallback()
	if err := rs.raftRouter.SendRaftCommand(request, cb); err != nil {
		return err
	}

	return rs.checkResponse(cb.WaitResp(), len(reqs))
}

func (rs *RaftStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	header := &raft_cmdpb.RaftRequestHeader{
		RegionId:    ctx.RegionId,
		Peer:        ctx.Peer,
		RegionEpoch: ctx.RegionEpoch,
		Term:        ctx.Term,
	}
	request := &raft_cmdpb.RaftCmdRequest{
		Header: header,
		Requests: []*raft_cmdpb.Request{{
			CmdType: raft_cmdpb.CmdType_Snap,
			Snap:    &raft_cmdpb.SnapRequest{},
		}},
	}
	cb := message.NewCallback()
	if err := rs.raftRouter.SendRaftCommand(request, cb); err != nil {
		return nil, err
	}

	resp := cb.WaitResp()
	if err := rs.checkResponse(resp, 1); err != nil {
		if cb.Txn != nil {
			cb.Txn.Discard()
		}
		return nil, err
	}
	if cb.Txn == nil {
		panic("can not found region snap")
	}
	if len(resp.Responses) != 1 {
		panic("wrong response count for snap cmd")
	}
	return NewRegionReader(cb.Txn, *resp.Responses[0].GetSnap().Region), nil
}


func (rs *RaftStorage) Raft(stream linkkvpb.LinkKv_RaftServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		rs.raftRouter.SendRaftMessage(msg)
	}
}

func (rs *RaftStorage) Snapshot(stream linkkvpb.LinkKv_SnapshotServer) error {
	var err error
	done := make(chan struct{})
	rs.snapWorker.Sender() <- & recvSnapTask {
		stream:	stream,
		callback: func(e error) {
			err = e
			close(done)
		},
	}
	<-done
	return err
}

func (rs *RaftStorage) Start() error {
	cfg := rs.config
	schedulerClient, err := scheduler_client.NewClient(strings.Split(cfg.SchedulerAddr, ","), "")
	if err != nil {
		return err
	}
	rs.raftRouter, rs.raftSystem = raftstore.CreateRaftstore(cfg)

	rs.resolveWorker = worker.NewWorker("resolver", &rs.wg)
	resolveSender := rs.resolveWorker.Sender()
	resolveRunner := newResolverRunner(schedulerClient)
	rs.resolveWorker.Start(resolveRunner)

	rs.snapManager = snap.NewSnapManager(filepath.Join(cfg.DBPath, "snap"))
	rs.snapWorker = worker.NewWorker("snap-worker", &rs.wg)
	snapSender := rs.snapWorker.Sender()
	snapRunner := newSnapRunner(rs.snapManager, rs.config, rs.raftRouter)
	rs.snapWorker.Start(snapRunner)

	raftClient := newRaftClient(cfg)
	trans := NewServerTransport(raftClient, snapSender, rs.raftRouter, resolveSender)

	rs.node = raftstore.NewNode(rs.raftSystem, rs.config, schedulerClient)
	err = rs.node.Start(context.TODO(), rs.engines, trans, rs.snapManager)
	if err != nil {
		return err
	}

	return nil
}
func (rs *RaftStorage) Stop() error {
	rs.snapWorker.Stop()
	rs.node.Stop()
	rs.resolveWorker.Stop()
	rs.wg.Wait()
	if err := rs.engines.Raft.Close(); err != nil {
		return err
	}

	if err := rs.engines.Kv.Close(); err != nil {
		return err
	}

	return nil
}
// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: linkkvpb.proto

package linkkvpb

import (
	"fmt"
	"math"

	proto "github.com/golang/protobuf/proto"

	_ "github.com/gogo/protobuf/gogoproto"

	coprocessor "miniLinkDB/proto/pkg/coprocessor"

	kvrpcpb "miniLinkDB/proto/pkg/kvrpcpb"

	raft_serverpb "miniLinkDB/proto/pkg/raft_serverpb"

	context "golang.org/x/net/context"

	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for LinkKv service

type LinkKvClient interface {
	// KV commands with mvcc/txn supported.
	KvGet(ctx context.Context, in *kvrpcpb.GetRequest, opts ...grpc.CallOption) (*kvrpcpb.GetResponse, error)
	KvScan(ctx context.Context, in *kvrpcpb.ScanRequest, opts ...grpc.CallOption) (*kvrpcpb.ScanResponse, error)
	KvPrewrite(ctx context.Context, in *kvrpcpb.PrewriteRequest, opts ...grpc.CallOption) (*kvrpcpb.PrewriteResponse, error)
	KvCommit(ctx context.Context, in *kvrpcpb.CommitRequest, opts ...grpc.CallOption) (*kvrpcpb.CommitResponse, error)
	KvCheckTxnStatus(ctx context.Context, in *kvrpcpb.CheckTxnStatusRequest, opts ...grpc.CallOption) (*kvrpcpb.CheckTxnStatusResponse, error)
	KvBatchRollback(ctx context.Context, in *kvrpcpb.BatchRollbackRequest, opts ...grpc.CallOption) (*kvrpcpb.BatchRollbackResponse, error)
	KvResolveLock(ctx context.Context, in *kvrpcpb.ResolveLockRequest, opts ...grpc.CallOption) (*kvrpcpb.ResolveLockResponse, error)
	// RawKV commands.
	RawGet(ctx context.Context, in *kvrpcpb.RawGetRequest, opts ...grpc.CallOption) (*kvrpcpb.RawGetResponse, error)
	RawPut(ctx context.Context, in *kvrpcpb.RawPutRequest, opts ...grpc.CallOption) (*kvrpcpb.RawPutResponse, error)
	RawDelete(ctx context.Context, in *kvrpcpb.RawDeleteRequest, opts ...grpc.CallOption) (*kvrpcpb.RawDeleteResponse, error)
	RawScan(ctx context.Context, in *kvrpcpb.RawScanRequest, opts ...grpc.CallOption) (*kvrpcpb.RawScanResponse, error)
	// Raft commands (linkkv <-> linkkv).
	Raft(ctx context.Context, opts ...grpc.CallOption) (LinkKv_RaftClient, error)
	Snapshot(ctx context.Context, opts ...grpc.CallOption) (LinkKv_SnapshotClient, error)
	// Coprocessor
	Coprocessor(ctx context.Context, in *coprocessor.Request, opts ...grpc.CallOption) (*coprocessor.Response, error)
}

type linkKvClient struct {
	cc *grpc.ClientConn
}

func NewLinkKvClient(cc *grpc.ClientConn) LinkKvClient {
	return &linkKvClient{cc}
}

func (c *linkKvClient) KvGet(ctx context.Context, in *kvrpcpb.GetRequest, opts ...grpc.CallOption) (*kvrpcpb.GetResponse, error) {
	out := new(kvrpcpb.GetResponse)
	err := c.cc.Invoke(ctx, "/linkkvpb.LinkKv/KvGet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *linkKvClient) KvScan(ctx context.Context, in *kvrpcpb.ScanRequest, opts ...grpc.CallOption) (*kvrpcpb.ScanResponse, error) {
	out := new(kvrpcpb.ScanResponse)
	err := c.cc.Invoke(ctx, "/linkkvpb.LinkKv/KvScan", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *linkKvClient) KvPrewrite(ctx context.Context, in *kvrpcpb.PrewriteRequest, opts ...grpc.CallOption) (*kvrpcpb.PrewriteResponse, error) {
	out := new(kvrpcpb.PrewriteResponse)
	err := c.cc.Invoke(ctx, "/linkkvpb.LinkKv/KvPrewrite", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *linkKvClient) KvCommit(ctx context.Context, in *kvrpcpb.CommitRequest, opts ...grpc.CallOption) (*kvrpcpb.CommitResponse, error) {
	out := new(kvrpcpb.CommitResponse)
	err := c.cc.Invoke(ctx, "/linkkvpb.LinkKv/KvCommit", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *linkKvClient) KvCheckTxnStatus(ctx context.Context, in *kvrpcpb.CheckTxnStatusRequest, opts ...grpc.CallOption) (*kvrpcpb.CheckTxnStatusResponse, error) {
	out := new(kvrpcpb.CheckTxnStatusResponse)
	err := c.cc.Invoke(ctx, "/linkkvpb.LinkKv/KvCheckTxnStatus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *linkKvClient) KvBatchRollback(ctx context.Context, in *kvrpcpb.BatchRollbackRequest, opts ...grpc.CallOption) (*kvrpcpb.BatchRollbackResponse, error) {
	out := new(kvrpcpb.BatchRollbackResponse)
	err := c.cc.Invoke(ctx, "/linkkvpb.LinkKv/KvBatchRollback", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *linkKvClient) KvResolveLock(ctx context.Context, in *kvrpcpb.ResolveLockRequest, opts ...grpc.CallOption) (*kvrpcpb.ResolveLockResponse, error) {
	out := new(kvrpcpb.ResolveLockResponse)
	err := c.cc.Invoke(ctx, "/linkkvpb.LinkKv/KvResolveLock", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *linkKvClient) RawGet(ctx context.Context, in *kvrpcpb.RawGetRequest, opts ...grpc.CallOption) (*kvrpcpb.RawGetResponse, error) {
	out := new(kvrpcpb.RawGetResponse)
	err := c.cc.Invoke(ctx, "/linkkvpb.LinkKv/RawGet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *linkKvClient) RawPut(ctx context.Context, in *kvrpcpb.RawPutRequest, opts ...grpc.CallOption) (*kvrpcpb.RawPutResponse, error) {
	out := new(kvrpcpb.RawPutResponse)
	err := c.cc.Invoke(ctx, "/linkkvpb.LinkKv/RawPut", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *linkKvClient) RawDelete(ctx context.Context, in *kvrpcpb.RawDeleteRequest, opts ...grpc.CallOption) (*kvrpcpb.RawDeleteResponse, error) {
	out := new(kvrpcpb.RawDeleteResponse)
	err := c.cc.Invoke(ctx, "/linkkvpb.LinkKv/RawDelete", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *linkKvClient) RawScan(ctx context.Context, in *kvrpcpb.RawScanRequest, opts ...grpc.CallOption) (*kvrpcpb.RawScanResponse, error) {
	out := new(kvrpcpb.RawScanResponse)
	err := c.cc.Invoke(ctx, "/linkkvpb.LinkKv/RawScan", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *linkKvClient) Raft(ctx context.Context, opts ...grpc.CallOption) (LinkKv_RaftClient, error) {
	stream, err := c.cc.NewStream(ctx, &_LinkKv_serviceDesc.Streams[0], "/linkkvpb.LinkKv/Raft", opts...)
	if err != nil {
		return nil, err
	}
	x := &linkKvRaftClient{stream}
	return x, nil
}

type LinkKv_RaftClient interface {
	Send(*raft_serverpb.RaftMessage) error
	CloseAndRecv() (*raft_serverpb.Done, error)
	grpc.ClientStream
}

type linkKvRaftClient struct {
	grpc.ClientStream
}

func (x *linkKvRaftClient) Send(m *raft_serverpb.RaftMessage) error {
	return x.ClientStream.SendMsg(m)
}

func (x *linkKvRaftClient) CloseAndRecv() (*raft_serverpb.Done, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(raft_serverpb.Done)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *linkKvClient) Snapshot(ctx context.Context, opts ...grpc.CallOption) (LinkKv_SnapshotClient, error) {
	stream, err := c.cc.NewStream(ctx, &_LinkKv_serviceDesc.Streams[1], "/linkkvpb.LinkKv/Snapshot", opts...)
	if err != nil {
		return nil, err
	}
	x := &linkKvSnapshotClient{stream}
	return x, nil
}

type LinkKv_SnapshotClient interface {
	Send(*raft_serverpb.SnapshotChunk) error
	CloseAndRecv() (*raft_serverpb.Done, error)
	grpc.ClientStream
}

type linkKvSnapshotClient struct {
	grpc.ClientStream
}

func (x *linkKvSnapshotClient) Send(m *raft_serverpb.SnapshotChunk) error {
	return x.ClientStream.SendMsg(m)
}

func (x *linkKvSnapshotClient) CloseAndRecv() (*raft_serverpb.Done, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(raft_serverpb.Done)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *linkKvClient) Coprocessor(ctx context.Context, in *coprocessor.Request, opts ...grpc.CallOption) (*coprocessor.Response, error) {
	out := new(coprocessor.Response)
	err := c.cc.Invoke(ctx, "/linkkvpb.LinkKv/Coprocessor", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for LinkKv service
type LinkKvServer interface {
	// KV commands with mvcc/txn supported.
	KvGet(context.Context, *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error)
	KvScan(context.Context, *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error)
	KvPrewrite(context.Context, *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error)
	KvCommit(context.Context, *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error)
	KvCheckTxnStatus(context.Context, *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error)
	KvBatchRollback(context.Context, *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error)
	KvResolveLock(context.Context, *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error)
	// RawKV commands.
	RawGet(context.Context, *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error)
	RawPut(context.Context, *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error)
	RawDelete(context.Context, *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error)
	RawScan(context.Context, *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error)
	// Raft commands (linkkv <-> linkkv).
	Raft(LinkKv_RaftServer) error
	Snapshot(LinkKv_SnapshotServer) error
	// Coprocessor
	Coprocessor(context.Context, *coprocessor.Request) (*coprocessor.Response, error)
}


func RegisterLinkKvServer(s *grpc.Server, srv LinkKvServer) {
	s.RegisterService(&_LinkKv_serviceDesc, srv)
}




func _LinkKv_KvGet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(kvrpcpb.GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LinkKvServer).KvGet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/linkkvpb.LinkKv/KvGet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LinkKvServer).KvGet(ctx, req.(*kvrpcpb.GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LinkKv_KvScan_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(kvrpcpb.ScanRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LinkKvServer).KvScan(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/linkkvpb.LinkKv/KvScan",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LinkKvServer).KvScan(ctx, req.(*kvrpcpb.ScanRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LinkKv_KvPrewrite_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(kvrpcpb.PrewriteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LinkKvServer).KvPrewrite(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/linkkvpb.LinkKv/KvPrewrite",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LinkKvServer).KvPrewrite(ctx, req.(*kvrpcpb.PrewriteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LinkKv_KvCommit_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(kvrpcpb.CommitRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LinkKvServer).KvCommit(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/linkkvpb.LinkKv/KvCommit",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LinkKvServer).KvCommit(ctx, req.(*kvrpcpb.CommitRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LinkKv_KvCheckTxnStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(kvrpcpb.CheckTxnStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LinkKvServer).KvCheckTxnStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/linkkvpb.LinkKv/KvCheckTxnStatus",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LinkKvServer).KvCheckTxnStatus(ctx, req.(*kvrpcpb.CheckTxnStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LinkKv_KvBatchRollback_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(kvrpcpb.BatchRollbackRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LinkKvServer).KvBatchRollback(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/linkkvpb.LinkKv/KvBatchRollback",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LinkKvServer).KvBatchRollback(ctx, req.(*kvrpcpb.BatchRollbackRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LinkKv_KvResolveLock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(kvrpcpb.ResolveLockRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LinkKvServer).KvResolveLock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/linkkvpb.LinkKv/KvResolveLock",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LinkKvServer).KvResolveLock(ctx, req.(*kvrpcpb.ResolveLockRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LinkKv_RawGet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(kvrpcpb.RawGetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LinkKvServer).RawGet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/linkkvpb.LinkKv/RawGet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LinkKvServer).RawGet(ctx, req.(*kvrpcpb.RawGetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LinkKv_RawPut_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(kvrpcpb.RawPutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LinkKvServer).RawPut(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/linkkvpb.LinkKv/RawPut",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LinkKvServer).RawPut(ctx, req.(*kvrpcpb.RawPutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LinkKv_RawDelete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(kvrpcpb.RawDeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LinkKvServer).RawDelete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/linkkvpb.LinkKv/RawDelete",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LinkKvServer).RawDelete(ctx, req.(*kvrpcpb.RawDeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LinkKv_RawScan_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(kvrpcpb.RawScanRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LinkKvServer).RawScan(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/linkkvpb.LinkKv/RawScan",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LinkKvServer).RawScan(ctx, req.(*kvrpcpb.RawScanRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LinkKv_Raft_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(LinkKvServer).Raft(&linkKvRaftServer{stream})
}

type LinkKv_RaftServer interface {
	SendAndClose(*raft_serverpb.Done) error
	Recv() (*raft_serverpb.RaftMessage, error)
	grpc.ServerStream
}

type linkKvRaftServer struct {
	grpc.ServerStream
}

func (x *linkKvRaftServer) SendAndClose(m *raft_serverpb.Done) error {
	return x.ServerStream.SendMsg(m)
}

func (x *linkKvRaftServer) Recv() (*raft_serverpb.RaftMessage, error) {
	m := new(raft_serverpb.RaftMessage)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _LinkKv_Snapshot_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(LinkKvServer).Snapshot(&linkKvSnapshotServer{stream})
}

type LinkKv_SnapshotServer interface {
	SendAndClose(*raft_serverpb.Done) error
	Recv() (*raft_serverpb.SnapshotChunk, error)
	grpc.ServerStream
}



type linkKvSnapshotServer struct {
	grpc.ServerStream
}

func (x *linkKvSnapshotServer) SendAndClose(m *raft_serverpb.Done) error {
	return x.ServerStream.SendMsg(m)
}

func (x *linkKvSnapshotServer) Recv() (*raft_serverpb.SnapshotChunk, error) {
	m := new(raft_serverpb.SnapshotChunk)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _LinkKv_Coprocessor_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(coprocessor.Request)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LinkKvServer).Coprocessor(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/linkkvpb.LinkKv/Coprocessor",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LinkKvServer).Coprocessor(ctx, req.(*coprocessor.Request))
	}
	return interceptor(ctx, in, info, handler)
}

var _LinkKv_serviceDesc = grpc.ServiceDesc{
	ServiceName: "linkkvpb.LinkKv",
	HandlerType: (*LinkKvServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "KvGet",
			Handler:    _LinkKv_KvGet_Handler,
		},
		{
			MethodName: "KvScan",
			Handler:    _LinkKv_KvScan_Handler,
		},
		{
			MethodName: "KvPrewrite",
			Handler:    _LinkKv_KvPrewrite_Handler,
		},
		{
			MethodName: "KvCommit",
			Handler:    _LinkKv_KvCommit_Handler,
		},
		{
			MethodName: "KvCheckTxnStatus",
			Handler:    _LinkKv_KvCheckTxnStatus_Handler,
		},
		{
			MethodName: "KvBatchRollback",
			Handler:    _LinkKv_KvBatchRollback_Handler,
		},
		{
			MethodName: "KvResolveLock",
			Handler:    _LinkKv_KvResolveLock_Handler,
		},
		{
			MethodName: "RawGet",
			Handler:    _LinkKv_RawGet_Handler,
		},
		{
			MethodName: "RawPut",
			Handler:    _LinkKv_RawPut_Handler,
		},
		{
			MethodName: "RawDelete",
			Handler:    _LinkKv_RawDelete_Handler,
		},
		{
			MethodName: "RawScan",
			Handler:    _LinkKv_RawScan_Handler,
		},
		{
			MethodName: "Coprocessor",
			Handler:    _LinkKv_Coprocessor_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Raft",
			Handler:       _LinkKv_Raft_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "Snapshot",
			Handler:       _LinkKv_Snapshot_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "linkkvpb.proto",
}

func init() { proto.RegisterFile("linkkvpb.proto", fileDescriptor_linkkvpb_71a6ae942ac295c5) }

var fileDescriptor_linkkvpb_71a6ae942ac295c5 = []byte{
	// 452 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x74, 0x93, 0xdb, 0x6e, 0xd3, 0x30,
	0x18, 0xc7, 0x5b, 0x09, 0xba, 0x62, 0x34, 0x18, 0x6e, 0x81, 0x2d, 0x8c, 0x20, 0xed, 0x8a, 0xab,
	0x22, 0x01, 0x12, 0x17, 0x1c, 0x24, 0x96, 0x4a, 0xbb, 0xf0, 0x90, 0x2a, 0x77, 0x5c, 0x23, 0xd7,
	0xfa, 0xd6, 0x56, 0xe9, 0xec, 0x60, 0x3b, 0x2e, 0x7d, 0x13, 0x9e, 0x83, 0xa7, 0xe0, 0x92, 0x47,
	0x40, 0xe5, 0x45, 0x50, 0x92, 0xd9, 0x39, 0x34, 0xbd, 0xb3, 0x7f, 0xff, 0x83, 0x5b, 0xc7, 0x1f,
	0x7a, 0x60, 0x96, 0x62, 0x13, 0xdb, 0x64, 0x36, 0x4a, 0x94, 0x34, 0x12, 0xf7, 0xdd, 0x3e, 0x38,
	0x8c, 0xad, 0x4a, 0xb8, 0x13, 0x82, 0x81, 0x62, 0xd7, 0xe6, 0x9b, 0x06, 0x65, 0x41, 0x79, 0xf8,
	0x88, 0xcb, 0x44, 0x49, 0x0e, 0x5a, 0x4b, 0x75, 0x8b, 0x86, 0x73, 0x39, 0x97, 0xf9, 0xf2, 0x55,
	0xb6, 0x2a, 0xe8, 0xeb, 0x5f, 0x07, 0xa8, 0x77, 0xb5, 0x14, 0x1b, 0x62, 0xf1, 0x5b, 0x74, 0x97,
	0xd8, 0x0b, 0x30, 0x78, 0x30, 0x72, 0x27, 0x5c, 0x80, 0xa1, 0xf0, 0x3d, 0x05, 0x6d, 0x82, 0x61,
	0x1d, 0xea, 0x44, 0x0a, 0x0d, 0x67, 0x1d, 0xfc, 0x0e, 0xf5, 0x88, 0x9d, 0x72, 0x26, 0x70, 0xe9,
	0xc8, 0xb6, 0x2e, 0xf7, 0xb8, 0x41, 0x7d, 0x30, 0x42, 0x88, 0xd8, 0x89, 0x82, 0xb5, 0x5a, 0x1a,
	0xc0, 0xc7, 0xde, 0xe6, 0x90, 0x2b, 0x38, 0x69, 0x51, 0x7c, 0xc9, 0x47, 0xd4, 0x27, 0x36, 0x92,
	0x37, 0x37, 0x4b, 0x83, 0x9f, 0x78, 0x63, 0x01, 0x5c, 0xc1, 0xd3, 0x1d, 0xee, 0xe3, 0x5f, 0xd1,
	0x11, 0xb1, 0xd1, 0x02, 0x78, 0x7c, 0xf5, 0x43, 0x4c, 0x0d, 0x33, 0xa9, 0xc6, 0x61, 0x69, 0xaf,
	0x09, 0xae, 0xee, 0xc5, 0x5e, 0xdd, 0xd7, 0x52, 0xf4, 0x90, 0xd8, 0x73, 0x66, 0xf8, 0x82, 0xca,
	0xd5, 0x6a, 0xc6, 0x78, 0x8c, 0x9f, 0xfb, 0x54, 0x8d, 0xbb, 0xd2, 0x70, 0x9f, 0xec, 0x3b, 0x2f,
	0xd1, 0x21, 0xb1, 0x14, 0xb4, 0x5c, 0x59, 0xb8, 0x94, 0x3c, 0xc6, 0xcf, 0x7c, 0xa4, 0x42, 0x5d,
	0xdf, 0x69, 0xbb, 0xe8, 0xdb, 0xde, 0xa3, 0x1e, 0x65, 0xeb, 0xec, 0x63, 0x97, 0xb7, 0x56, 0x80,
	0xdd, 0x5b, 0x73, 0xbc, 0x11, 0x9e, 0xa4, 0x8d, 0xf0, 0x24, 0x6d, 0x0f, 0xe7, 0xdc, 0x87, 0xc7,
	0xe8, 0x1e, 0x65, 0xeb, 0x31, 0xac, 0xc0, 0x00, 0x3e, 0xa9, 0xfa, 0x0a, 0xe6, 0x2a, 0x82, 0x36,
	0xc9, 0xb7, 0x7c, 0x42, 0x07, 0x94, 0xad, 0xf3, 0x67, 0x57, 0x3b, 0xab, 0xfa, 0xf2, 0x8e, 0x77,
	0x85, 0xca, 0x5f, 0xb8, 0x43, 0xd9, 0xb5, 0xc1, 0xc1, 0xa8, 0x3e, 0x3d, 0x19, 0xfc, 0x02, 0x5a,
	0xb3, 0x39, 0x04, 0x83, 0x86, 0x36, 0x96, 0x02, 0xce, 0x3a, 0x2f, 0xbb, 0xf8, 0x33, 0xea, 0x4f,
	0x05, 0x4b, 0xf4, 0x42, 0x1a, 0x7c, 0xda, 0x30, 0x39, 0x21, 0x5a, 0xa4, 0x22, 0xde, 0x5f, 0xf1,
	0x01, 0xdd, 0x8f, 0xca, 0x09, 0xc5, 0xc3, 0x51, 0x75, 0x5e, 0xcb, 0xd1, 0xa9, 0x53, 0xf7, 0xeb,
	0xcf, 0x8f, 0x7e, 0x6f, 0xc3, 0xee, 0x9f, 0x6d, 0xd8, 0xfd, 0xbb, 0x0d, 0xbb, 0x3f, 0xff, 0x85,
	0x9d, 0x59, 0x2f, 0x9f, 0xe6, 0x37, 0xff, 0x03, 0x00, 0x00, 0xff, 0xff, 0xc2, 0x6c, 0xc6, 0xd2,
	0x36, 0x04, 0x00, 0x00,
}
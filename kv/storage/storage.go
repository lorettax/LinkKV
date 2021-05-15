package storage

import (
	"miniLinkDB/kv/util/engine_util"
	"miniLinkDB/proto/pkg/kvrpcpb"
)

// Storage 代表 LinkDB的面向内部的服务器部分 处理来自其它服务器的发送和接收

type Storage interface {
	Start() error
	Stop() error
	Write(ctx *kvrpcpb.Context, batch []Modify) error
	Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}

type StorageReader interface {
	// if key doesn't exist, return nil for the value
	GetCF(cf string, key []byte) ([]byte, error)
	IterCF(cf string) engine_util.DBIterator
}

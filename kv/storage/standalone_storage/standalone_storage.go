package standalone_storage

import (
	"miniLinkDB/kv/config"
	"miniLinkDB/kv/storage"
	"miniLinkDB/proto/pkg/kvrpcpb"
)

type StandAloneStorage struct {
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return nil
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	return nil
}

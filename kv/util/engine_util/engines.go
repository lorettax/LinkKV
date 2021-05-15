package engine_util

import (
	"github.com/Connor1996/badger"
	"miniLinkDB/log"
	"os"
)

// 引擎会保留对unistore使用的引擎的引用和数据,所有引擎都是badge键值数据库  “路径”字段是存储数据的文件系统路径
type Engines struct {
	//
	Kv 	*badger.DB
	KvPath string
	// Metadata used used by Raft
	Raft 	*badger.DB
	RaftPath string
}

func NewEngines(kvEngine, raftEngine *badger.DB, kvPath, raftPath string) *Engines {
	return &Engines{
		Kv: kvEngine,
		KvPath: kvPath,
		Raft: raftEngine,
		RaftPath: raftPath,
	}
}

func (en *Engines) WriteKV(wb *WriteBatch) error {
	return wb.WriteToDB(en.Kv)
}

func (en *Engines) WriteRaft(wb *WriteBatch) error {
	return wb.WriteToDB(en.Raft)
}

func (en *Engines) Close() error {
	if err := en.Kv.Close(); err != nil {
		return err
	}
	if err := en.Raft.Close(); err != nil {
		return err
	}
	return nil
}


func (en *Engines) Destroy() error {
	if err := en.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(en.KvPath); err != nil {
		return err
	}
	if err := os.RemoveAll(en.RaftPath); err != nil {
		return err
	}
	return nil
}

// CreateDB在子路径的磁盘上创建一个新的Badger DB
func CreateDB(path string, raft bool) *badger.DB {
	opts := badger.DefaultOptions
	if raft {
		opts.ValueThreshold = 0
	}

	opts.Dir = path
	opts.ValueDir = opts.Dir
	if err := os.MkdirAll(opts.Dir, os.ModePerm); err != nil {
		log.Fatal(err)
	}
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

package raft_storage

import (
	"github.com/Connor1996/badger"
	"miniLinkDB/kv/raftstore/util"
	"miniLinkDB/kv/util/engine_util"
	"miniLinkDB/proto/pkg/metapb"
)

type RegionReader struct {
	txn 	*badger.Txn
	region 	*metapb.Region
}

func NewRegionReader(txn *badger.Txn, region metapb.Region) *RegionReader  {
	return &RegionReader{
		txn: txn,
		region: &region,
	}
}

func (r *RegionReader) GetCF(cf string, key []byte) ([]byte, error) {
	if err := util.CheckKeyInRegion(key, r.region); err != nil {
		return nil, err
	}
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *RegionReader) IterCF(cf string) engine_util.DBIterator {
	return NewRegionIterator(engine_util.NewCFIterator(cf, r.txn), r.region)
}

func (r *RegionReader) Close() {
	r.txn.Discard()
}

// RegionIterator包装一个db迭代器，并且只允许它在区域中进行迭代。它的行为就像基础db仅包含一个区域。
type RegionIterator struct {
	iter *engine_util.BadgerIterator
	region *metapb.Region
}

func NewRegionIterator(iter *engine_util.BadgerIterator, region *metapb.Region) *RegionIterator {
	return &RegionIterator{
		iter:	iter,
		region:	region,
	}
}

func (it *RegionIterator) Item() engine_util.DBItem {
	return it.iter.Item()
}

func (it *RegionIterator) Valid() bool {
	if !it.iter.Valid() || engine_util.ExceedEndKey(it.iter.Item().Key(), it.region.EndKey) {
		return false
	}
	return true
}

func (it *RegionIterator) ValidForPrefix(prefix []byte) bool {
	if !it.iter.ValidForPrefix(prefix) || engine_util.ExceedEndKey(it.iter.Item().Key(), it.region.EndKey) {
		return false
	}
	return true
}

func (it *RegionIterator) Close() {
	it.iter.Close()
}

func (it *RegionIterator) Next() {
	it.iter.Next()
}

func (it *RegionIterator) Seek(key []byte) {
	if 	err := util.CheckKeyInRegion(key, it.region); err != nil {
		panic(err)
	}
	it.iter.Seek(key)
}

func (it *RegionIterator) Rewind() {
	it.iter.Rewind()
}
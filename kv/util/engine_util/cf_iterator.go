package engine_util

import "github.com/Connor1996/badger"

type CFItem struct {
	item 	*badger.Item
	prefixLen	int
}

func (i *CFItem) String() string {
	return i.item.String()
}

func (i *CFItem) Key() []byte {
	return i.item.Key()[i.prefixLen:]
}

func (i *CFItem) KeyCopy(dst []byte) []byte {
	return i.item.KeyCopy(dst)[i.prefixLen:]
}

func (i *CFItem) Version() uint64 {
	return i.item.Version()
}

func (i *CFItem) IsEmpty() bool {
	return i.item.IsEmpty()
}

func (i *CFItem) Value() ([]byte, error) {
	return i.item.Value()
}

func (i *CFItem) ValueSize() int {
	return i.item.ValueSize()
}

func (i *CFItem) ValueCopy(dst []byte) ([]byte, error) {
	return i.item.ValueCopy(dst)
}

func (i *CFItem) IsDeleted() bool {
	return i.item.IsDeleted()
}

func (i *CFItem) EstimatedSize() int64 {
	return i.item.EstimatedSize()
}

func (i *CFItem) UserMeta() []byte {
	return i.item.UserMeta()
}

type BadgerIterator struct {
	iter 	*badger.Iterator
	prefix 	string
}

func NewCFIterator(cf string, txn *badger.Txn) *BadgerIterator {
	return &BadgerIterator{
		iter:   txn.NewIterator(badger.DefaultIteratorOptions),
		prefix: cf + "_",
	}
}

func (it *BadgerIterator) Item() DBItem {
	return &CFItem{
		item: it.iter.Item(),
		prefixLen: len(it.prefix),
	}
}

func (it *BadgerIterator) Valid() bool {
	return it.iter.ValidForPrefix([]byte(it.prefix))
}

func (it *BadgerIterator) ValidForPrefix(prefix []byte) bool {
	return it.iter.ValidForPrefix(append(prefix, []byte(it.prefix)...))
}

func (it *BadgerIterator) Close() {
	it.iter.Close()
}

func (it *BadgerIterator) Next() {
	it.iter.Next()
}

func (it *BadgerIterator) Seek(key []byte) {
	it.iter.Seek(append([]byte(it.prefix), key...))
}

func (it *BadgerIterator) Rewind() {
	it.iter.Rewind()
}

type DBIterator interface {
	// Item返回当前键值对的指针
	Item() DBItem
	// 迭代完成后 有效返回false
	Valid() bool
	// 将迭代器前进一个，始终在Next之后检查it.Valid，以确保您可以访问有效的it.Item()
	Next()
	// Seek将寻求提供的密钥（如果存在）。如果不存在，它将寻求大于提供的下一个最小密钥。
	Seek([]byte)
	// Close the iterator
	Close()
}

type DBItem interface {
	// Key returns the key.
	Key() []byte
	// KeyCopy返回该项目的密钥的副本，并将其写入dst slice。如果通过nil或dst的容量不足，则将分配并返回一个新的片。
	KeyCopy(dst []byte) []byte
	// Value retrieves the value of the item.
	Value() ([]byte, error)
	// ValueSize返回value 的大小
	ValueSize() int
	// ValueCopy 从值日志中返回该项目的值的副本，并将其写入dst slice。如果通过nil或dst的容量不足，则将分配并返回一个新的片
	ValueCopy(dst []byte) ([]byte, error)
}












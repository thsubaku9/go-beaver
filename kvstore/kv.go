package kvstore

import "beaver/btreeplus"

type KV struct {
	Path string
	df   int
	tree btreeplus.BTree
	// todo more
}

func (db *KV) Open() error {
	return nil
}

func (db *KV) Get(key btreeplus.ByteArr) (val btreeplus.ByteArr, exists bool) {
	k, v := db.tree.Get(key)
	return v, k != nil
}

func (db *KV) Set(key, val btreeplus.ByteArr) error {
	if err := db.tree.Insert(key, val); err != nil {
		return nil
	}
	return performFileUpdate(db)
}

func (db *KV) Del(key btreeplus.ByteArr) (isDeleted bool, err error) {
	if isDeleted, err = db.tree.Delete(key); err != nil {
		return isDeleted, err
	}

	return isDeleted, performFileUpdate(db)
}

func performFileUpdate(db *KV) error {
	return nil
}

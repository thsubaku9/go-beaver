package kvstore

import (
	"beaver/btreeplus"
	"beaver/helpers"
	"fmt"
	"syscall"
)

type KV struct {
	Path string
	fd   int
	tree btreeplus.BTree
	mmap struct {
		totalSize uint64
		chunks    [][]byte
	}
	page struct {
		flushedSize uint64
		temp        []btreeplus.BNode
	}
	// todo more
}

func extendMmap(db *KV, size uint64) error {
	if size <= db.mmap.totalSize {
		return nil
	}

	incrementSize := max(db.mmap.totalSize, 64<<10)

	for db.mmap.totalSize+incrementSize < size {
		incrementSize += incrementSize
	}

	chunk, err := syscall.Mmap(db.fd, int64(db.mmap.totalSize), int(incrementSize),
		syscall.PROT_READ, syscall.MAP_SHARED) // ro

	if err != nil {
		return fmt.Errorf("mmap :%w", err)
	}

	db.mmap.totalSize += incrementSize
	db.mmap.chunks = append(db.mmap.chunks, chunk)

	return nil
}

func (db *KV) Open() error {
	db.tree = btreeplus.NewBTree(db.pageRead, db.pageAppend, db.pageDelete)
	return nil
}

func (db *KV) pageRead(ptr uint64) btreeplus.BNode {
	start := uint64(0)

	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/btreeplus.BTREE_PAGE_SIZE
		if ptr < end {
			offset := btreeplus.BTREE_PAGE_SIZE * (ptr - start)
			return chunk[offset : offset+btreeplus.BTREE_PAGE_SIZE]
		}
		start = end
	}
	panic("bad ptr")
}

func (db *KV) pageAppend(bnode btreeplus.BNode) uint64 {
	ptr := db.page.flushedSize + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, bnode)
	return ptr
}

func (db *KV) pageDelete(ptr uint64) {

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
	return helpers.ErrMap(db, []func(*KV) error{
		writePages,
		fsync, // forces previous and next step to be ordered (page cache stuff)
		updateRoot,
		fsync,
	})
}

func writePages(db *KV) error {
	// todo
	return nil
}

func updateRoot(db *KV) error {
	return nil
}

func fsync(db *KV) error {
	return syscall.Fsync(db.fd)
}

package kvstore

import (
	"beaver/btreeplus"
	"beaver/helpers"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

type KV struct {
	Path    string
	filePtr *os.File
	fd      int
	tree    btreeplus.BTree
	mmap    struct {
		totalMmapSizeBytes uint64
		totalFileSizeBytes uint64
		chunks             [][]byte
	}
	page struct {
		flushedCount uint64
		temp         []btreeplus.BNode
	}
	// todo more
}

func extendFile(db *KV, size uint64) error {
	if db.mmap.totalFileSizeBytes >= size {
		return nil
	}

	incrementSize := max(db.mmap.totalFileSizeBytes, 64<<10)

	for db.mmap.totalFileSizeBytes+incrementSize < size {
		incrementSize += incrementSize
	}

	db.filePtr.Truncate(int64(db.mmap.totalFileSizeBytes) + int64(incrementSize))
	db.mmap.totalFileSizeBytes += incrementSize

	return nil
}

func extendMmap(db *KV, size uint64) error {
	if size <= db.mmap.totalMmapSizeBytes {
		return nil
	}

	incrementSize := max(db.mmap.totalMmapSizeBytes, 64<<10)

	for db.mmap.totalMmapSizeBytes+incrementSize < size {
		incrementSize += incrementSize
	}

	chunk, err := unix.Mmap(db.fd, int64(db.mmap.totalMmapSizeBytes), int(incrementSize),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED) // rw

	if err != nil {
		return fmt.Errorf("mmap :%w", err)
	}

	db.mmap.totalMmapSizeBytes += incrementSize
	db.mmap.chunks = append(db.mmap.chunks, chunk)

	return nil
}

func (db *KV) Open() error {
	filePtr, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	db.filePtr = filePtr

	fileStat, _ := filePtr.Stat()
	fileStat.Size()

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
	ptr := db.page.flushedCount + uint64(len(db.page.temp))
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
	size := (db.page.flushedCount + uint64(len(db.page.temp))) * btreeplus.BTREE_PAGE_SIZE
	// page extension also needs to be done (via truncate)
	if err := extendFile(db, size); err != nil {
		return err
	}

	if err := extendMmap(db, size); err != nil {
		return err
	}

	offset := db.page.flushedCount * btreeplus.BTREE_PAGE_SIZE
	// todo -> implement flock here
	// pwrite because pwritev unsupported on macos :(
	for _, pageToFlush := range db.page.temp {
		unix.Pwrite(db.fd, pageToFlush, int64(offset))
		offset += uint64(len(pageToFlush))
	}

	db.page.flushedCount += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]
	return nil
}

func updateRoot(db *KV) error {
	return nil
}

func fsync(db *KV) error {
	return unix.Fsync(db.fd)
}

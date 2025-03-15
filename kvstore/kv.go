package kvstore

import (
	"beaver/btreeplus"
	"beaver/helpers"
	"encoding/binary"
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
		toDelete     []uint64
	}
	lastUpdateFailed bool
}

// OS HELPER CODE

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

func (db *KV) Close() error {
	for _, mmapChunk := range db.mmap.chunks {
		helpers.Assert(unix.Munmap(mmapChunk) == nil)
	}
	return db.filePtr.Close()
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
	db.page.toDelete = append(db.page.toDelete, ptr)
}

func updateOrRevert(db *KV, meta []byte) error {
	if db.lastUpdateFailed {
		updateRoot(db)
		fsync(db)
		db.lastUpdateFailed = false
	}
	// 2-phase update
	err := performFileUpdate(db)
	// revert on error
	if err != nil {
		// the in-memory states can be reverted immediately to allow reads
		loadMeta(db, meta)
		// discard temporaries
		db.page.temp = db.page.temp[:0]
		// the on-disk meta page is in an unknown state;
		// mark it to be rewritten on later recovery.
		db.lastUpdateFailed = true
	}
	return err
}

func (db *KV) Get(key btreeplus.ByteArr) (val btreeplus.ByteArr, exists bool) {
	k, v := db.tree.Get(key)
	return v, k != nil
}

func (db *KV) Set(key, val btreeplus.ByteArr) error {
	meta := saveMeta(db)
	if err := db.tree.Insert(key, val); err != nil {
		return err
	}
	return updateOrRevert(db, meta)
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

func fsync(db *KV) error {
	return unix.Fsync(db.fd)
}

// META RELATED FNS

const DB_SIG = "BEAVER01"

// | sig | root_ptr | page_used |
// | 8B |    8B    |     8B    |
func saveMeta(db *KV) []byte {
	var data [24]byte
	copy(data[:8], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[8:], db.tree.GetRoot())
	binary.LittleEndian.PutUint64(data[16:], db.page.flushedCount)
	return data[:]
}

func loadMeta(db *KV, data []byte) {
	helpers.Assert(DB_SIG == string(data[0:8]))
	db.tree.SetRoot(binary.LittleEndian.Uint64(data[8:]))
	db.page.flushedCount = binary.LittleEndian.Uint64(data[16:])
}

func readRoot(db *KV, fileSize uint64) error {
	if fileSize == 0 {
		db.page.flushedCount = 1
		return nil
	}

	data := db.mmap.chunks[0]
	loadMeta(db, data)
	return nil
}

func updateRoot(db *KV) error {
	if _, err := unix.Pwrite(db.fd, saveMeta(db), 0); err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	return nil
}

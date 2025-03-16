package kvstore

import (
	"beaver/btreeplus"
	"beaver/helpers"
	"encoding/binary"
)

// node format:
// | next | pointers | unused |
// |  8B  |   n*8B   |   ...  |
type LNode []byte

const FREE_LIST_HEADER_SIZE = 8
const POINTER_SIZE = 8
const FREE_LIST_CAP = (btreeplus.BTREE_PAGE_SIZE - FREE_LIST_HEADER_SIZE) / 8

// getters & setters
func (node LNode) getNext() uint64 {
	return binary.LittleEndian.Uint64(node[0:FREE_LIST_HEADER_SIZE])
}

func (node LNode) setNext(next uint64) {
	binary.LittleEndian.PutUint64(node[0:FREE_LIST_HEADER_SIZE], next)
}

func (node LNode) getPtr(idx int) uint64 {
	offset := FREE_LIST_HEADER_SIZE + idx*POINTER_SIZE
	return binary.LittleEndian.Uint64(node[offset : offset+POINTER_SIZE])
}
func (node LNode) setPtr(idx int, ptr uint64) {
	offset := FREE_LIST_HEADER_SIZE + idx*POINTER_SIZE
	binary.LittleEndian.PutUint64(node[offset:offset+POINTER_SIZE], ptr)
}

type Freelist struct {
	// callbacks for managing on-disk pages
	get func(uint64) []byte // read a page
	new func([]byte) uint64 // append a new page
	set func(uint64) []byte // update an existing page
	// persisted data in the meta page
	headPage uint64
	headSeq  uint64
	tailPage uint64
	tailSeq  uint64
	// in-memory states
	maxSeq uint64 // saved `tailSeq` to prevent consuming newly added items
}

func NewFreelist(get func(uint64) []byte,
	new func([]byte) uint64,
	set func(uint64) []byte) Freelist {
	return Freelist{
		get: get,
		new: new,
		set: set,
	}
}

func (fl *Freelist) PopHead() (uint64, bool) {
	ptr, head := flPop(fl)
	if head == 0 {
		return 0, false
	}

	fl.PushTail(head)
	return ptr, true
}

func (fl *Freelist) PushTail(ptr uint64) {
	LNode(fl.set(fl.tailPage)).setPtr(seq2idx(fl.tailSeq), ptr)
	fl.tailSeq++

	if seq2idx(fl.tailSeq) == 0 {
		next, head := flPop(fl)

		// nothing to pop hence new node allocation
		if next == 0 {
			next = fl.new(btreeplus.NewBnode())
		}

		LNode(fl.set(fl.tailPage)).setNext(next)
		fl.tailPage = next
		if head != 0 {
			LNode(fl.set(fl.tailPage)).setPtr(0, head)
			fl.tailSeq++
		}
	}
}

func (fl *Freelist) SetMaxSeq() {
	fl.maxSeq = fl.tailSeq
}

func flPop(fl *Freelist) (ptr uint64, head uint64) {
	if fl.headSeq == fl.maxSeq {
		return 0, 0
	}

	node := LNode(fl.get(fl.headPage))
	ptr = node.getPtr(seq2idx(fl.headSeq))

	fl.headSeq++

	if seq2idx(fl.headSeq) == 0 {
		head, fl.headPage = fl.headPage, node.getNext()
		helpers.Assert(fl.headPage != 0)
	}

	return
}

func seq2idx(seq uint64) int {
	return int(seq % FREE_LIST_CAP)
}

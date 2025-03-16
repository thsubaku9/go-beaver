package kvstore

import (
	"beaver/btreeplus"
	"encoding/binary"
)

// node format:
// |pointers | unused |
// | n*8B    |   ...  |
type LNode []byte

// header format:
// |magic| num of pointers | head position | tail position | prev file pointer | next file point
// | 4B  | 8B | 8B | 8B | 8B | 8B

const FL_SIG = "FL01"
const FREE_LIST_HEADER_SIZE = len(FL_SIG) + 5*HEADER_ENTRY_SIZE
const HEADER_ENTRY_SIZE = 8
const FREE_LIST_CAP = (btreeplus.BTREE_PAGE_SIZE - FREE_LIST_HEADER_SIZE) / 8

func PopulateFreeListNode(lnode LNode, pointerCount, headPos, tailPos, prevFP, nextFp uint64) {
	copy(lnode[0:len(FL_SIG)], []byte(FL_SIG))

}

func (lnode LNode) getTotalPointers() uint64 {
	return binary.LittleEndian.Uint64(lnode[len(FL_SIG) : len(FL_SIG)+HEADER_ENTRY_SIZE])
}

func (lnode LNode) setTotalPointers(v uint64) {
	binary.LittleEndian.PutUint64(lnode[len(FL_SIG):len(FL_SIG)+HEADER_ENTRY_SIZE], v)
}

func (lnode LNode) getHeadPosition() uint64 {
	return binary.LittleEndian.Uint64(lnode[len(FL_SIG)+HEADER_ENTRY_SIZE : len(FL_SIG)+2*HEADER_ENTRY_SIZE])
}

func (lnode LNode) setHeadPosition(v uint64) {
	binary.LittleEndian.PutUint64(lnode[len(FL_SIG)+HEADER_ENTRY_SIZE:len(FL_SIG)+2*HEADER_ENTRY_SIZE], v)
}

func (lnode LNode) getTailPosition() uint64 {
	return binary.LittleEndian.Uint64(lnode[len(FL_SIG)+(2*HEADER_ENTRY_SIZE) : len(FL_SIG)+(3*HEADER_ENTRY_SIZE)])
}

func (lnode LNode) setTailPosition(v uint64) {
	binary.LittleEndian.PutUint64(lnode[len(FL_SIG)+(2*HEADER_ENTRY_SIZE):len(FL_SIG)+(3*HEADER_ENTRY_SIZE)], v)
}

func (lnode LNode) prevFilePointer() (uint64, bool) {
	val := binary.LittleEndian.Uint64(lnode[len(FL_SIG)+(3*HEADER_ENTRY_SIZE) : len(FL_SIG)+(4*HEADER_ENTRY_SIZE)])

	return val, val == 0
}

func (lnode LNode) setPrevFilePointer(v uint64) {
	binary.LittleEndian.PutUint64(lnode[len(FL_SIG)+(3*HEADER_ENTRY_SIZE):len(FL_SIG)+(4*HEADER_ENTRY_SIZE)], v)
}

func (lnode LNode) nextFilePointer() (uint64, bool) {
	val := binary.LittleEndian.Uint64(lnode[len(FL_SIG)+(4*HEADER_ENTRY_SIZE) : len(FL_SIG)+(5*HEADER_ENTRY_SIZE)])

	return val, val == 0
}

func (lnode LNode) setNextFilePointer(v uint64) {
	binary.LittleEndian.PutUint64(lnode[len(FL_SIG)+(4*HEADER_ENTRY_SIZE):len(FL_SIG)+(5*HEADER_ENTRY_SIZE)], v)
}

func (node LNode) getPtr(idx int) uint64 {
	return binary.LittleEndian.Uint64(node[FREE_LIST_HEADER_SIZE+idx*HEADER_ENTRY_SIZE:])
}

func (node LNode) setPtr(idx int, ptr uint64) {
	binary.LittleEndian.PutUint64(node[FREE_LIST_HEADER_SIZE+idx*HEADER_ENTRY_SIZE:FREE_LIST_HEADER_SIZE+(idx+1)*HEADER_ENTRY_SIZE], ptr)
}

// func (lnode LNode) isNodeFull() bool {
// 	return BTREE_PAGE_SIZE == FREE_LIST_HEADER_SIZE + lnode.getTotalPointers() *
// }

type Freelist struct {
	// callbacks for managing on-disk pages
	get                func(uint64) btreeplus.BNode  // read a page
	new                func(btreeplus.BNode) uint64  // append a new page
	set                func(uint64, btreeplus.BNode) // update an existing page
	currentPagePointer uint64
	// persisted data in the meta page
	headPage uint64
	headSeq  uint64
	tailPage uint64
	tailSeq  uint64
	// in-memory states
	maxSeq uint64 // saved `tailSeq` to prevent consuming newly added items
}

func NewFreelist(get func(uint64) btreeplus.BNode,
	new func(btreeplus.BNode) uint64,
	set func(uint64, btreeplus.BNode)) Freelist {
	return Freelist{
		get: get,
		new: new,
		set: set,
	}

}

func (fl *Freelist) SetParamsFromLNode(lnode LNode, currentPagePointer uint64) {
	fl.headSeq = lnode.getHeadPosition()
	fl.tailSeq = lnode.getTailPosition()
	fl.headPage, _ = lnode.prevFilePointer()
	fl.tailPage, _ = lnode.nextFilePointer()
	fl.maxSeq = fl.tailSeq
	fl.currentPagePointer = currentPagePointer
}

func seq2idx(seq uint64) int {
	return int(seq) % FREE_LIST_CAP
}

func (fl *Freelist) PushTail(ptr uint64) {
	/*
		1. if tail wraps to head, move to a new page
		2. else just push value and update tail position
		3. update the mem location
	*/

	if seq2idx(fl.tailSeq+1) == seq2idx(fl.headSeq) {

		for seq2idx(fl.tailSeq+1) == seq2idx(fl.headSeq) {
			if fl.tailSeq != 0 {
				fl.SetParamsFromLNode(LNode(fl.get(fl.tailPage)), fl.tailPage)
			} else {
				// create new page
				pageLNode := LNode(btreeplus.NewBnode())
				pagePtr := fl.new(btreeplus.BNode(pageLNode))

				// update current page with next ptr ref
				curPageLNode := LNode(fl.get(fl.currentPagePointer))
				curPageLNode.setNextFilePointer(pagePtr)

				fl.set(pagePtr, btreeplus.BNode(pageLNode))

				// update back ptr for new page
				pageLNode.setPrevFilePointer(fl.currentPagePointer)
				fl.currentPagePointer = pagePtr
			}
		}

	}

	curpageNode := LNode(fl.get(fl.currentPagePointer))
	curpageNode.setPtr(seq2idx(fl.tailSeq), ptr)
	fl.tailSeq++
	curpageNode.setTailPosition(fl.tailSeq)

	fl.set(fl.currentPagePointer, btreeplus.BNode(curpageNode))

}

func (fl *Freelist) PopHead() (ptr uint64, exists bool) {
	/*
		1. if head moves into tail, return no exists
		2. else return the head value and update head position
		3. update the mem location
	*/

	for seq2idx(fl.headSeq+1) == seq2idx(fl.tailSeq) {
		if fl.headPage != 0 {
			fl.SetParamsFromLNode(LNode(fl.get(fl.headPage)), fl.headPage)
		} else {
			return 0, false

		}
	}

	curpageNode := LNode(fl.get(fl.currentPagePointer))
	ptr = curpageNode.getPtr(seq2idx(fl.headSeq))
	exists = true
	fl.headSeq++
	curpageNode.setHeadPosition(fl.headSeq)
	fl.set(fl.currentPagePointer, btreeplus.BNode(curpageNode))
	return
}

/* free list needs to be able to do the following ->
1. maintain enteries of free list info
2. perform seeks
3. perform updates refresh
*/

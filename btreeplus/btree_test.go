package btreeplus

import (
	"beaver/helpers"
	"unsafe"
)

type BtreeSimulator struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func NewBTS() *BtreeSimulator {
	pages := make(map[uint64]BNode)
	return &BtreeSimulator{
		ref:   make(map[string]string),
		pages: pages,
		tree: BTree{
			del: func(u uint64) {
				helpers.Assert(pages[u] != nil)
				delete(pages, u)
			},
			get: func(u uint64) BNode {
				v, ok := pages[u]
				helpers.Assert(ok)
				return v
			},
			new: func(b BNode) uint64 {
				helpers.Assert(b.nbytes() <= BTREE_PAGE_SIZE)
				ptr := uint64(uintptr(unsafe.Pointer(&b[0])))
				helpers.Assert(pages[ptr] == nil)
				pages[ptr] = b
				return ptr
			},
		},
	}
}

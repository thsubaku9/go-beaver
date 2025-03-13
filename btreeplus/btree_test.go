package btreeplus

import (
	"beaver/helpers"
	"fmt"
	"unsafe"
)

type BtreeContainer struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func NewBTS() *BtreeContainer {
	pages := make(map[uint64]BNode)
	return &BtreeContainer{
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

func (c *BtreeContainer) Add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *BtreeContainer) Del(key string) (bool, error) {
	delete(c.ref, key)
	return c.tree.Delete(ByteArr(key))
}

func (c *BtreeContainer) PrintTree() {
	// fmt.Printf("Root page: %d\n", c.pages[c.tree.root])
	fmt.Println("Pages:")
	for pt, node := range c.pages {
		fmt.Printf("Pointer addr: %v | Pointer type : %s | Pointer data: %s\n", pt, NodeType(node.btype()), node[:node.nbytes()])
	}
}

/*

Things to test ->
The structure is valid.
	Keys are sorted.
	Node sizes are within limits.
The data matches a reference. We used a map to capture each update.
*/

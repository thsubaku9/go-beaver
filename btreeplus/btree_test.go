package btreeplus

import (
	"beaver/helpers"
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
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

func (c *BtreeContainer) Get(key string) (ByteArr, ByteArr) {
	return c.tree.Get(ByteArr(key))
}

func (c *BtreeContainer) PrintTree() {
	// fmt.Printf("Root page: %d\n", c.pages[c.tree.root])
	fmt.Println("Pages:")
	for pt, node := range c.pages {
		fmt.Printf("Pointer addr: %v | Pointer type : %s | Pointer data: %s\n", pt, NodeType(node.btype()), node[:node.nbytes()])
	}
}

func TestStandardBCreation(t *testing.T) {
	treeContainer := NewBTS()

	treeContainer.Add("k1", "mickey1")
	k, v := treeContainer.Get("k1")
	assert.Equal(t, k, ByteArr("k1"))
	assert.Equal(t, v, ByteArr("mickey1"))
}

func TestMultiBInsert(t *testing.T) {
	treeContainer := NewBTS()

	for i := 1; i < 10; i++ {
		treeContainer.Add(fmt.Sprintf("k%d", i), fmt.Sprintf("mickey%d", i))
	}
	k, v := treeContainer.Get("k1")
	assert.Equal(t, k, ByteArr("k1"))
	assert.Equal(t, v, ByteArr("mickey1"))
}

func TestKeyDeletion(t *testing.T) {
	treeContainer := NewBTS()

	for i := 1; i < 10; i++ {
		treeContainer.Add(fmt.Sprintf("k%d", i), fmt.Sprintf("mickey%d", i))
	}

	k, v := treeContainer.Get("k9")
	assert.Equal(t, "k9", string(k))
	assert.Equal(t, "mickey9", string(v))

	res, err := treeContainer.Del("k9")
	assert.Nil(t, err)
	assert.True(t, res)

	k, v = treeContainer.Get("k9")
	assert.Nil(t, k)
	assert.Nil(t, v)

	k, v = treeContainer.Get("k8")
	assert.Equal(t, "k8", string(k))
	assert.Equal(t, "mickey8", string(v))

	// same deletion should be a no-op

	res, err = treeContainer.Del("k9")
	assert.NotNil(t, err)
	assert.False(t, res)
}

func TestPageChain(t *testing.T) {
	treeContainer := NewBTS()

	for i := 1; i < 10; i++ {
		treeContainer.Add(fmt.Sprintf("k%d", i), fmt.Sprintf("mickey%d", i))
	}

	chain, res := treeContainer.tree._internalsFetchNodeChain(ByteArr("k5"))

	assert.True(t, res, "Page should exist")
	assert.NotEmpty(t, chain, "chain should be there")
	assert.Len(t, chain, 1, "only one page should be there")
	page0 := chain[0]
	assert.Equal(t, LeafNode, NodeType(page0.btype()))
	assert.Equal(t, uint16(10), page0.nkeys()) // num of keys = n + 1 (because of sentinal value)
}

/*

Things to test ->
The structure is valid.
	Keys are sorted.
	Node sizes are within limits.
The data matches a reference. We used a map to capture each update.
*/

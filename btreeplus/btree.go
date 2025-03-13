package btreeplus

import (
	"bytes"
	"fmt"
)

const (
	KEY_LIM = 20
	VAL_LIM = 1000
)

type BTree struct {
	// root pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // read data from a page number
	new func(BNode) uint64 // allocate a new page number with data
	del func(uint64)       // deallocate a page number
}

func checkLimit(key, val ByteArr) error {
	if len(key) > KEY_LIM {
		return fmt.Errorf("Key limit exceeded")
	}

	if len(val) > VAL_LIM {
		return fmt.Errorf("Val limit exceeded")
	}

	return nil
}

func nodeReplaceKidN(tree *BTree, new, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.setHeader(uint16(InternalNode), old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)

	for i, node := range kids {
		k, _ := node.getKeyAndVal(0)
		nodeAppendKV(new, idx+uint16(i), tree.new(node), k, nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// The extra size allows it to exceed 1 page temporarily.
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))

	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case uint16(LeafNode): // leaf node
		k, _ := node.getKeyAndVal(idx)

		if bytes.Equal(key, k) {
			leafUpsert(new, node, idx, key, val, 0x01)
		} else {
			leafUpsert(new, node, idx+1, key, val, 0x00)
		}
	case uint16(InternalNode): // internal node, walk into the child node
		kptr := node.getPtr(idx)
		knode := treeInsert(tree, tree.get(kptr), key, val)

		nsplit, split := nodeSplit3(knode)

		// remove old page since cow
		tree.del(kptr)

		// need to adjust current page based on split that has happened to child page
		nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
	}
	return new
}

func (tree *BTree) Insert(key, val ByteArr) error {

	if err := checkLimit(key, val); err != nil {
		return err
	}

	// sentinel value
	if tree.root == 0 {
		root := NewBnode()
		root.setHeader(uint16(LeafNode), 2)
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return nil
	}

	node := treeInsert(tree, tree.get(tree.root), key, val)

	nsplit, split := nodeSplit3(node)
	defer tree.del(tree.root)

	if nsplit > 1 {
		root := NewBnode()
		root.setHeader(uint16(InternalNode), nsplit)

		for i, knode := range split[:nsplit] {
			ptr := tree.new(knode)
			key, _ := knode.getKeyAndVal(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}

	return nil
}

func (tree *BTree) Delete(key ByteArr) (bool, error) {
	return false, nil
}

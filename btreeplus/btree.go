package btreeplus

import (
	"beaver/helpers"
	"bytes"
	"fmt"
)

type BTree struct {
	// root pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // read data from a page number
	new func(BNode) uint64 // allocate a new page number with data
	del func(uint64)       // deallocate a page number
}

func NewBTree(get func(uint64) BNode,
	new func(BNode) uint64,
	del func(uint64)) BTree {
	return BTree{
		get: get,
		new: new,
		del: del,
	}
}

func checkLimit(key, val ByteArr) error {
	if len(key) > BTREE_MAX_KEY_SIZE {
		return fmt.Errorf("key limit exceeded")
	}

	if len(val) > BTREE_MAX_VAL_SIZE {
		return fmt.Errorf("val limit exceeded")
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
	switch NodeType(node.btype()) {
	case LeafNode: // leaf node
		k, _ := node.getKeyAndVal(idx)

		if bytes.Equal(key, k) {
			leafUpsert(new, node, idx, key, val, 0x01)
		} else {
			leafUpsert(new, node, idx+1, key, val, 0x00)
		}
	case InternalNode: // internal node, walk into the child node
		kptr := node.getPtr(idx)
		knode := treeInsert(tree, tree.get(kptr), key, val)

		nsplit, split := nodeSplit3(knode)

		// remove old page since cow
		defer tree.del(kptr)

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
		newRoot := NewBnode()
		newRoot.setHeader(uint16(InternalNode), nsplit)

		for i, knode := range split[:nsplit] {
			pagePtr := tree.new(knode)
			splitKey, _ := knode.getKeyAndVal(0)
			nodeAppendKV(newRoot, uint16(i), pagePtr, splitKey, nil)
		}
		tree.root = tree.new(newRoot)
	} else {
		tree.root = tree.new(split[0])
	}
	return nil
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updatedKid BNode) (int, BNode) {
	if updatedKid.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.nbytes() + updatedKid.nbytes() - HEADER_SIZE
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling // left
		}
	}

	if idx+1 < node.nkeys() {
		sibling := tree.get(node.getPtr(idx + 1))
		merged := sibling.nbytes() + updatedKid.nbytes() - HEADER_SIZE
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling // right
		}
	}
	return 0, BNode{}
}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key ByteArr) BNode {
	idx := nodeLookupLE(node, key)

	switch NodeType(node.btype()) {
	case LeafNode:
		k, _ := node.getKeyAndVal(idx)
		if !bytes.Equal(key, k) {
			return nil
		}
		new := NewBnode()
		leafDelete(new, node, idx)
		return new
	case InternalNode:
		return nodeDelete(tree, node, idx, key)
	}

	// this won't occur
	return nil
}

// nodeDelete takes care of recursing the internal nodes + merging
func nodeDelete(tree *BTree, node BNode, idx uint16, key ByteArr) BNode {
	childptr := node.getPtr(idx)
	updatedChildPage := treeDelete(tree, tree.get(childptr), key)

	if len(updatedChildPage) == 0 {
		return BNode{}
	}

	defer tree.del(childptr)
	new := NewBnode()

	mergeDir, sibling := shouldMerge(tree, node, idx, updatedChildPage)

	switch {
	case mergeDir == 0 && updatedChildPage.nkeys() == 0:
		helpers.Assert(node.nkeys() == 1 && idx == 0)
		new.setHeader(uint16(InternalNode), 0)
	case mergeDir == 0 && updatedChildPage.nkeys() > 0:
		nodeReplaceKidN(tree, new, node, idx, updatedChildPage)
	case mergeDir == -1: // left dir
		merged := NewBnode()
		nodeMerge(merged, sibling, updatedChildPage)
		defer tree.del(node.getPtr(idx - 1))
		newKey, _ := merged.getKeyAndVal(0)
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), newKey)
	case mergeDir == 1: // right dir
		merged := NewBnode()
		nodeMerge(merged, updatedChildPage, sibling)
		tree.del(node.getPtr(idx + 1))
		newKey, _ := merged.getKeyAndVal(0)
		nodeReplace2Kid(new, node, idx, tree.new(merged), newKey)
	}
	return new
}

func (tree *BTree) Delete(key ByteArr) (bool, error) {

	helpers.Assert(len(key) != 0)
	helpers.Assert(len(key) < BTREE_MAX_KEY_SIZE)

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated) == 0 {
		return false, fmt.Errorf("key not found")
	}

	defer tree.del(tree.root)

	if NodeType(updated.btype()) == InternalNode && updated.nkeys() == 1 {
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}

	return true, nil
}

func (tree *BTree) Get(key ByteArr) (retKey, retVal ByteArr) {
	retKey = key
	helpers.Assert(len(key) != 0 && len(key) < BTREE_MAX_KEY_SIZE)
	if tree.root == 0 {
		return nil, nil
	}

	var node BNode
	for node = tree.get(tree.root); NodeType(node.btype()) != LeafNode; {
		idx := nodeLookupLE(node, key)
		kaddr := node.getPtr(idx)
		if kaddr == 0 {
			return nil, nil
		}
		node = tree.get(kaddr)
	}

	idx := nodeLookupLE(node, key)

	_k, _v := node.getKeyAndVal(idx)
	if bytes.Equal(_k, key) {
		return _k, _v
	}

	return nil, nil
}

func (tree *BTree) GetRoot() uint64 {
	return tree.root
}

func (tree *BTree) SetRoot(rootPtr uint64) {
	tree.root = rootPtr
}

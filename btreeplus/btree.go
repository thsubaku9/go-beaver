package btreeplus

import "bytes"

type BTree struct {
	// root pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // read data from a page number
	new func(BNode) uint64 // allocate a new page number with data
	del func(uint64)       // deallocate a page number
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// The extra size allows it to exceed 1 page temporarily.
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	// where to insert the key?
	idx := nodeLookupLE(node, key) // node.getKey(idx) <= key
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

package btreeplus

import (
	"beaver/helpers"
	"bytes"
)

func (tree *BTree) _internalsFetchNodeChain(key ByteArr) ([]BNode, bool) {
	helpers.Assert(len(key) != 0 && len(key) < BTREE_MAX_KEY_SIZE)
	var bnodeChain []BNode = make([]BNode, 0)

	if tree.root == 0 {
		return bnodeChain, false
	}

	var node BNode
	for node = tree.get(tree.root); NodeType(node.btype()) != LeafNode; {
		bnodeChain = append(bnodeChain, node)
		idx := nodeLookupLE(node, key)
		kaddr := node.getPtr(idx)
		if kaddr == 0 {
			return bnodeChain, false
		}
		node = tree.get(kaddr)
	}

	bnodeChain = append(bnodeChain, node)
	idx := nodeLookupLE(node, key)

	_k, _ := node.getKeyAndVal(idx)
	if bytes.Equal(_k, key) {
		return bnodeChain, true
	}

	return bnodeChain, false

}

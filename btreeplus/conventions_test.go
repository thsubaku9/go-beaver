package btreeplus

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHeaderVal(t *testing.T) {
	node := BNode(make([]byte, BTREE_PAGE_SIZE))
	node.setHeader(uint16(LeafNode), 0)

	node.btype()
	assert.Equal(t, NodeType(node.btype()), LeafNode)
	assert.Equal(t, node.nkeys(), uint16(0))
}

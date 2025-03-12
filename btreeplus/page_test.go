package btreeplus

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHeaderVal(t *testing.T) {
	node := NewBnode()
	node.setHeader(uint16(LeafNode), 0)

	node.btype()
	assert.Equal(t, NodeType(node.btype()), LeafNode)
	assert.Equal(t, node.nkeys(), uint16(0))
}

func TestInsertionKVLeaf(t *testing.T) {
	node := NewBnode()
	node.setHeader(uint16(LeafNode), 3)

	entries := []struct {
		k string
		v string
	}{
		{
			k: "k0",
			v: "lionel messi",
		},
		{
			k: "k1",
			v: "cristiano ronaldo",
		},
		{
			k: "k2",
			v: "gareth bale",
		},
	}

	for idx, e := range entries {
		nodeAppendKV(node, uint16(idx), 0, ByteArr(e.k), ByteArr(e.v))
	}

	for idx, e := range entries {
		r_k, r_v := node.getKeyAndVal(uint16(idx))
		assert.Equal(t, r_k, ByteArr(e.k), fmt.Sprintf("Keys unequal for idx %v", idx))
		assert.Equal(t, r_v, ByteArr(e.v), fmt.Sprintf("Values unequal for idx %v", idx))
	}

}

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

func TestNodeLookup(t *testing.T) {
	node := NewBnode()
	node.setHeader(uint16(LeafNode), 5)

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
		{
			k: "k3",
			v: "neymar santos",
		},
		{
			k: "k4",
			v: "lebron james",
		},
	}

	for idx, e := range entries {
		nodeAppendKV(node, uint16(idx), 0, ByteArr(e.k), ByteArr(e.v))
	}

	idx := nodeLookupLE(node, ByteArr("k3"))

	k, v := node.getKeyAndVal(idx)

	assert.GreaterOrEqual(t, string(k), "k3")
	assert.Contains(t, []string{entries[3].v, entries[4].v}, string(v))
}

func TestNodeSplit2(t *testing.T) {
	node := NewBnode()
	node.setHeader(uint16(LeafNode), 2)

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
	}

	for idx, e := range entries {
		nodeAppendKV(node, uint16(idx), 0, ByteArr(e.k), ByteArr(e.v))
	}

	lnode, rnode := NewBnode(), NewBnode()
	nodeSplit2(lnode, rnode, node)

	assert.Equal(t, uint16(1), lnode.nkeys())
	assert.Equal(t, uint16(1), rnode.nkeys())
}

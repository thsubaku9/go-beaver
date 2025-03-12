package btreeplus

import (
	"beaver/helpers"
	"bytes"
	"encoding/binary"
	"fmt"
)

type NodeType uint16

const (
	InternalNode NodeType = iota
	LeafNode
)

func (n NodeType) String() string {
	switch n {
	case InternalNode:
		return "InternalNode"
	case LeafNode:
		return "LeafNode"
	default:
		return "NA"
	}
}

// ByteArr references to either the key/value variable value
type ByteArr []byte

// BNode references to a logical/physical page
type BNode []byte

/*
Internal Node
| type | nkeys |  pointers  |  offsets   | key-values | unused |
|  2B  |   2B  | nkeys × 8B | nkeys × 4B |     ...    |        |

Leaf Node
| key_size | val_size | key | val |
|    2B    |    2B    | ... | ... |
*/

const (
	BASE               = 0
	NODE_TYPE_SIZE     = 2
	NKEYS_SIZE         = 2
	HEADER_SIZE        = NODE_TYPE_SIZE + NKEYS_SIZE
	POINTER_SIZE       = 8
	OFFSET_SIZE        = 2
	KEY_SIZE           = 2
	VAL_SIZE           = 2
	KV_HEADER_SIZE     = KEY_SIZE + VAL_SIZE
	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

func init() {
	node1max := HEADER_SIZE + 1*POINTER_SIZE + 1*OFFSET_SIZE + KEY_SIZE + VAL_SIZE + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	helpers.Assert(node1max <= BTREE_PAGE_SIZE) // maximum KV
}

func NewBnode() BNode {
	return BNode(make([]byte, BTREE_PAGE_SIZE))
}

// header fns
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[BASE:NODE_TYPE_SIZE])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[BASE+NODE_TYPE_SIZE : 4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[BASE:NODE_TYPE_SIZE], btype)
	binary.LittleEndian.PutUint16(node[BASE+NODE_TYPE_SIZE:BASE+NODE_TYPE_SIZE+NKEYS_SIZE], nkeys)
}

// ptr fns
func (node BNode) getPtr(idx uint16) uint64 {
	helpers.Assert(idx < node.nkeys())
	pos_ptr := HEADER_SIZE + idx*POINTER_SIZE
	return binary.LittleEndian.Uint64(node[pos_ptr:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	helpers.Assert(idx < node.nkeys())
	pos_ptr := HEADER_SIZE + idx*POINTER_SIZE
	binary.LittleEndian.PutUint64(node[pos_ptr:], val)
}

// offset fns wrt starting of kv entry block
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	pos := HEADER_SIZE + POINTER_SIZE*node.nkeys() + OFFSET_SIZE*(idx-1)
	return binary.LittleEndian.Uint16(node[pos:])
}

func (node BNode) setOffset(idx uint16, offsetVal uint16) {
	if idx == 0 {
		return
	}
	pos := HEADER_SIZE + POINTER_SIZE*node.nkeys() + OFFSET_SIZE*(idx-1)
	binary.LittleEndian.PutUint16(node[pos:], offsetVal)
}

// provides position of kv entry block wrt buffer start
func (node BNode) getKvStartPosition() uint16 {
	return HEADER_SIZE + POINTER_SIZE*node.nkeys() + OFFSET_SIZE*node.nkeys()
}

// obtaining kv position given the index
func (node BNode) kvPos(idx uint16) uint16 {
	helpers.Assert(idx <= node.nkeys())
	return node.getOffset(idx) + node.getKvStartPosition()
}

func (node BNode) getKeyAndVal(idx uint16) (ByteArr, ByteArr) {
	helpers.Assert(idx < node.nkeys())
	pos := node.kvPos(idx)

	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+KEY_SIZE:])
	return ByteArr(node[pos+KV_HEADER_SIZE:][:klen]), ByteArr(node[pos+KV_HEADER_SIZE+klen:][:vlen])
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

func nodeAppendKV(bnode BNode, idx uint16, ptr uint64, key ByteArr, val ByteArr) {
	bnode.setPtr(idx, ptr)

	pos := bnode.kvPos(idx)

	binary.LittleEndian.PutUint16(bnode[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(bnode[pos+KEY_SIZE:], uint16(len(val)))

	copy(bnode[pos+KV_HEADER_SIZE:], key)
	copy(bnode[pos+KV_HEADER_SIZE+uint16(len(key)):], val)

	bnode.setOffset(idx+1, bnode.getOffset(idx)+KV_HEADER_SIZE+uint16((len(key)+len(val))))
}

func nodeAppendRange(new, old BNode, dstNew, srcOld, n uint16) {

	for i := uint16(0); i < n; i++ {
		dst, src := dstNew+i, srcOld+i
		oldPtr := old.getPtr(src)
		k, v := old.getKeyAndVal(src)

		nodeAppendKV(new, dst, oldPtr, k, v)
	}
}

func leafUpsert(new, old BNode, idx uint16, key, val ByteArr, isUpdate uint16) {
	new.setHeader(uint16(LeafNode), old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+(isUpdate&0x01), old.nkeys()-(idx+(isUpdate&0x01)))
}

func nodeLookupLE(node BNode, key ByteArr) uint16 {
	for i := uint16(0); i < node.nkeys(); i++ {
		nodeKey, _ := node.getKeyAndVal(i)
		switch bytes.Compare(nodeKey, key) {
		case 0:
			return i
		case 1:
			return i - 1
		}
	}

	return node.nkeys() - 1
}

// todok
/*
idx := nodeLookupLE(node, key)  // node.getKey(idx) <= key
if bytes.Equal(key, node.getKey(idx)) {
    leafUpdate(new, node, idx, key, val)   // found, update it
} else {
    leafInsert(new, node, idx+1, key, val) // not found. insert
}
*/

func nodeSplit2(left, right, old BNode) {
	helpers.Assert(old.nkeys() >= 2)
	nleft := old.nkeys() / 2
	left_bytes := func() uint16 {
		return HEADER_SIZE + POINTER_SIZE*nleft + OFFSET_SIZE*nleft +
			old.getOffset(nleft)
	}

	for left_bytes() > BTREE_PAGE_SIZE {
		nleft--
	}

	helpers.Assert(nleft >= 1)

	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes() + KV_HEADER_SIZE
	}

	for right_bytes() > BTREE_PAGE_SIZE {
		nleft++
	}

	helpers.Assert(nleft < old.nkeys())
	nright := old.nkeys() - nleft
	// new nodes
	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
	// NOTE: the left half may be still too big
	helpers.Assert(right.nbytes() <= BTREE_PAGE_SIZE)

}

// split a node if it's too big. the results are 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // not split
	}

	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // might be split later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // 2 nodes
	}

	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	helpers.Assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right} // 3 nodes
}

func Run() {
	node := BNode(make([]byte, BTREE_PAGE_SIZE))
	node.setHeader(uint16(LeafNode), 2)
	// ^type       ^ number of keys
	nodeAppendKV(node, 0, 0, []byte("k1"), []byte("hi"))
	// ^ 1st KV
	nodeAppendKV(node, 1, 0, []byte("k3"), []byte("hello"))
	// ^ 2nd KV

	fmt.Println(NodeType(node.btype()))
	fmt.Println(node.nkeys())
	k, v := node.getKeyAndVal(0)
	fmt.Printf("%s := %s\n", k, v)

	k, v = node.getKeyAndVal(1)
	fmt.Printf("%s := %s\n", k, v)
	fmt.Println(node.nbytes())
}

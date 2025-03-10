package btreeplus

import (
	"beaver/helpers"
	"encoding/binary"
	"fmt"
)

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
	OFFSET_SIZE        = 4
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

// offset fns
func (node BNode) getOffset(idx uint16) uint16 {
	pos := HEADER_SIZE + POINTER_SIZE*node.nkeys() + OFFSET_SIZE*(idx)
	return binary.LittleEndian.Uint16(node[pos:])
}

func (node BNode) setOffset(idx uint16, offsetVal uint16) {
	pos := HEADER_SIZE + POINTER_SIZE*node.nkeys() + OFFSET_SIZE*(idx)
	binary.LittleEndian.PutUint16(node[pos:], offsetVal)
}

// obtaining kv position given the index
func (node BNode) kvPos(idx uint16) uint16 {
	helpers.Assert(idx <= node.nkeys())
	return node.getOffset(idx)
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

func Run() {
	node := BNode(make([]byte, BTREE_PAGE_SIZE))
	node.setHeader(uint16(Leaf), 2)
	// ^type       ^ number of keys
	nodeAppendKV(node, 0, 0, []byte("k1"), []byte("hi"))
	// ^ 1st KV
	nodeAppendKV(node, 1, 0, []byte("k3"), []byte("hello"))
	// ^ 2nd KV

	fmt.Println(NodeType(node.btype()))
	fmt.Println(node.nkeys())
	k, v := node.getKeyAndVal(1)
	fmt.Printf("%s := %s\n", k, v)
}

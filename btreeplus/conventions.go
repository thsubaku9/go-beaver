package btreeplus

import (
	"beaver/helpers"
	"encoding/binary"
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

// header getters
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:NODE_TYPE_SIZE])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[0+NODE_TYPE_SIZE : 4])
}

// header setter
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:NODE_TYPE_SIZE], btype)
	binary.LittleEndian.PutUint16(node[0+NODE_TYPE_SIZE:4], nkeys)
}

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

func (node BNode) getOffset(idx uint16) uint16 {
	pos := HEADER_SIZE + POINTER_SIZE*node.nkeys() + OFFSET_SIZE*(idx)
	return binary.LittleEndian.Uint16(node[pos:])
}

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

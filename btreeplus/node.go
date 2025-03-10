package btreeplus

type NodeType int

const (
	Internal NodeType = iota
	Leaf
)

type Node interface {
	GetNodeType() NodeType
}

type BaseNode struct {
	keys []ByteArr
}

type InternalNode struct {
	BaseNode
	children []*Node
}

func (i InternalNode) GetNodeType() NodeType {
	return Internal
}

type LeafNode struct {
	BaseNode
	vals []ByteArr
}

func (l LeafNode) GetNodeType() NodeType {
	return Leaf
}

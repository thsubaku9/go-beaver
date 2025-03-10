package btreeplus

type NodeType uint16

const (
	Internal NodeType = iota + 1
	Leaf
)

func (n NodeType) String() string {
	switch n {
	case Internal:
		return "Internal"
	case Leaf:
		return "Leaf"
	default:
		return "NA"
	}
}

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

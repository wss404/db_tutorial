package db

import (
	"unsafe"
)

type NodeType int

const (
	NodeInternal NodeType = iota
	NodeLeaf
)

type uint8_t uint8

/* Common Node Header Layout */
const (
	NodeTypeSize         = uint32_t(unsafe.Sizeof(uint8_t(0)))
	NodeTypeOffset       uint32_t = 0
	IsRootSize            = uint32_t(unsafe.Sizeof(uint8_t(0)))
	IsRootOffset         = NodeTypeSize
	ParentPointerSize     = uint32_t(unsafe.Sizeof(uint32_t(0)))
	ParentPointerOffset  = IsRootOffset + IsRootSize
	CommonNodeHeaderSize = NodeTypeSize + IsRootSize + ParentPointerSize
)

/* Leaf Node Header Layout */
const (
	LeafNodeNumCellsSize  = uint32_t(unsafe.Sizeof(uint32_t(0)))
	LeafNodeNumCellsOffset  = CommonNodeHeaderSize
	LeafNodeHeaderSize  = CommonNodeHeaderSize + LeafNodeNumCellsSize
)

/* Leaf Node Body Layout */
const (
	LeafNodeKeySize = uint32_t(unsafe.Sizeof(uint32_t(0)))
	LeafNodeKeyOffset uint32_t = 0
	LeafNodeValueSize = RowSize
	LeafNodeValueOffset = LeafNodeKeyOffset + LeafNodeKeySize
	LeafNodeCellSize = LeafNodeKeySize + LeafNodeValueSize
	LeafNodeSpaceForCells = PageSize - LeafNodeHeaderSize
	LeafNodeMaxCells = LeafNodeSpaceForCells / LeafNodeCellSize
)

type Node struct {
	// common header
	nodeType NodeType
	isRoot uint8_t
	parentPointer *Node
	// leaf node header
	numCells uint32_t
	// leaf node body
	cells []*Cell
}

type Cell struct {
	key uint32_t
	value *Row
}

func (n *Node) leafNodeNumCells() *uint32_t {
	if n.nodeType == NodeLeaf {
		return &(n.numCells)
	}
	panic("trying to get numCells from internal node")
}

func (n *Node) leafNodeCell(cellNum uint32_t) *Cell {
	if n.numCells != uint32_t(len(n.cells)) {
		panic("Node data error")
	}
	if n.numCells < cellNum {
		panic("index out of range")
	}
	return n.cells[cellNum]
}

func (n *Node) leafNodeKey(cellNum uint32_t) *uint32_t {
	return &(n.leafNodeCell(cellNum).key)
}

func (n *Node) leafNodeValue(cellNum uint32_t) *Row {
	return n.leafNodeCell(cellNum).value
}

func (n *Node) initializeLeafNode() {
	*n.leafNodeNumCells() = 0
}
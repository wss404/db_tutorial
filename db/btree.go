package db

import (
	"unsafe"
)

type NodeType int

const (
	NodeInternal NodeType = iota
	NodeLeaf
)

/* Common Node Header Layout */
const (
	NodeTypeSize         = uint32_t(unsafe.Sizeof(uint8(0)))
	NodeTypeOffset       uint32_t = 0
	IsRootSize            = uint32_t(unsafe.Sizeof(uint8(0)))
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

func (n *Node) name()  {
	
}
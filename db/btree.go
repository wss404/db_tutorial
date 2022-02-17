package db

import (
	"fmt"
	"math"
	"os"
	"unsafe"
)

type PageType uint8_t

const (
	PageLeaf PageType = iota
	PageInternal
)

type uint8_t uint8

/* Common Node Header Layout */
const (
	NodeTypeSize                  = uint32_t(unsafe.Sizeof(PageLeaf))
	NodeTypeOffset       uint32_t = 0
	IsRootSize                    = uint32_t(unsafe.Sizeof(uint8_t(0)))
	IsRootOffset                  = NodeTypeSize
	ParentPointerSize             = uint32_t(unsafe.Sizeof(uintptr(0)))
	ParentPointerOffset           = IsRootOffset + IsRootSize
	CommonNodeHeaderSize          = NodeTypeSize + IsRootSize + ParentPointerSize
)

/* Leaf Node Header Layout */
const (
	LeafNodeNumCellsSize   = uint32_t(unsafe.Sizeof(uint32_t(0)))
	LeafNodeNumCellsOffset = CommonNodeHeaderSize
	LeafNodeHeaderSize     = CommonNodeHeaderSize + LeafNodeNumCellsSize
)

/* Leaf Node Body Layout */
const (
	LeafNodeKeySize                = uint32_t(unsafe.Sizeof(uint32_t(0)))
	LeafNodeKeyOffset     uint32_t = 0
	LeafNodeValueSize              = RowSize
	LeafNodeValueOffset            = LeafNodeKeyOffset + LeafNodeKeySize
	LeafNodeCellSize               = LeafNodeKeySize + LeafNodeValueSize
	LeafNodeSpaceForCells          = PageSize - LeafNodeHeaderSize
	LeafNodeMaxCells               = LeafNodeSpaceForCells / LeafNodeCellSize
)

/* Internal Node Header Layout */
const (
	InternalNodeNumKeysSize = uint32_t(unsafe.Sizeof(uint32_t(0)))
	InternalNodeNumKeysOffset = CommonNodeHeaderSize
	InternalNodeRightChildSize = uint32_t(unsafe.Sizeof(uintptr(0)))
	InternalNodeRightChildOffset = InternalNodeNumKeysOffset + InternalNodeNumKeysSize
	InternalNodeHeaderSize = CommonNodeHeaderSize + InternalNodeNumKeysSize + InternalNodeRightChildSize
)

/* Internal Node Body Layout */
const (
	InternalNodeKeySize = unsafe.Sizeof(uint32_t(0))
	InternalNodeChildSize = unsafe.Sizeof(uintptr(0))
	InternalNodeCellSize = InternalNodeKeySize + InternalNodeChildSize
)

const (
	LeafNodeRightSplitCount = (LeafNodeMaxCells + 1) / 2
	LeafNodeLeftSpiltCount = LeafNodeMaxCells + 1 - LeafNodeRightSplitCount
)

type Cell struct {
	key   uint32_t
	value Row
}

func (p *Page) leafNodeNumCells() *uint32_t {
	if p.pageType == PageLeaf {
		return &(p.numCells)
	}
	panic("trying to get numCells from internal node")
}

func (p *Page) leafNodeCell(cellNum uint32_t) *Cell {
	return &(p.cells[cellNum])
}

func (p *Page) leafNodeKey(cellNum uint32_t) *uint32_t {
	return &(p.leafNodeCell(cellNum).key)
}

func (p *Page) leafNodeValue(cellNum uint32_t) *Row {
	return &(p.leafNodeCell(cellNum).value)
}

func (p *Page) initializeLeafNode() {
	p.setPageType(PageLeaf) // actually, it's not necessary since default PageType is PageLeaf
	*p.leafNodeNumCells() = 0
}

func (p *Page) printLeafNode()  {
	numCells := *p.leafNodeNumCells()
	fmt.Printf("leaf (size %d)\n", numCells)
	for i := uint32_t(0); i < numCells; i++ {
		key := *p.leafNodeKey(i)
		fmt.Printf("  - %d : %d\n", i, key)
	}
}

func (c *Cursor) leafNodeSplitAndInsert(key uint32_t, value *Row)  {
	oldPage := c.table.pager.getPage(c.pageNum)
	newPageNum := c.table.pager.getUnusedPageNum()
	newPage := c.table.pager.getPage(newPageNum)
	newPage.initializeLeafNode()

	// TODO:有待改进
	for i := int32(LeafNodeMaxCells); i >= 0; i-- {
		var destinationPage *Page
		destinationPage = oldPage
		if i >= int32(LeafNodeLeftSpiltCount) {
			destinationPage = newPage
		}
		indexWithinCell := uint32_t(i % int32(LeafNodeLeftSpiltCount))
		destinationCell := destinationPage.leafNodeCell(indexWithinCell)

		if i == int32(c.cellNum) {
			value.serializeRow(unsafe.Pointer(destinationCell))
		} else if i > int32(c.cellNum) {
			oldPage.leafNodeCell(uint32_t(i-1)).moveTo(destinationCell)
		} else {
			oldPage.leafNodeCell(uint32_t(i)).moveTo(destinationCell)
		}
	}

	*(oldPage.leafNodeNumCells()) = LeafNodeLeftSpiltCount
	*(newPage.leafNodeNumCells()) = LeafNodeRightSplitCount

	if oldPage.isRoot != 0 {
		c.table.createNewRoot(newPageNum)
	} else {
		fmt.Println("Need to implement updating parent after split.")
		os.Exit(ExitFailure)
	}
}

func (p *Pager) getUnusedPageNum() uint32_t { return p.numPages }

func (t *Table) createNewRoot(rightChildPageNum uint32_t)  {
	root := t.pager.getPage(t.rootPageNum)
	//rightChild := t.pager.getPage(rightChildPageNum)
	leftChildPageNum := t.pager.getUnusedPageNum()
	leftChild := t.pager.getPage(leftChildPageNum)

	copy((*[PageSize]byte)(unsafe.Pointer(leftChild))[:],
		(*[PageSize]byte)(unsafe.Pointer(root))[:])
	leftChild.isRoot = 0

	root.initializeInternalNode()
	root.isRoot = 1
	*(root.internalNodeNumKeys()) = 1
	*(root.internalNodeChild(0)) = leftChildPageNum
	leftChildMaxKey := leftChild.getMaxKey()
	*(root.internalNodeKey(0)) = leftChildMaxKey
	*(root.internalNodeRightChild()) = rightChildPageNum
}

func (p *Page) internalNodeNumKeys() *uint32_t {
	if p.pageType == PageInternal {
		return &(p.numCells)
	}
	panic("trying to get numCells from internal node")
}

func (p *Page) internalNodeRightChild()  {

}
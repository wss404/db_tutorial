package db

import (
	"fmt"
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
	ParentPointerSize             = uint32_t(unsafe.Sizeof(uint32_t(0)))
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
	InternalNodeNumKeysSize      = uint32_t(unsafe.Sizeof(uint32_t(0)))
	InternalNodeNumKeysOffset    = CommonNodeHeaderSize
	InternalNodeRightChildSize   = uint32_t(unsafe.Sizeof(uint32_t(0)))
	InternalNodeRightChildOffset = InternalNodeNumKeysOffset + InternalNodeNumKeysSize
	InternalNodeHeaderSize       = CommonNodeHeaderSize + InternalNodeNumKeysSize + InternalNodeRightChildSize
)

/* Internal Node Body Layout */
const (
	InternalNodeKeySize       = unsafe.Sizeof(uint32_t(0))
	InternalNodeChildSize     = unsafe.Sizeof(uint32_t(0))
	InternalNodeCellSize      = InternalNodeKeySize + InternalNodeChildSize
	InternalNodeSpaceForCells = PageSize - InternalNodeHeaderSize
	InternalNodeMaxCells      = InternalNodeSpaceForCells / InternalNodeSpaceForCells
)

const (
	LeafNodeRightSplitCount = (LeafNodeMaxCells + 1) / 2
	LeafNodeLeftSpiltCount  = LeafNodeMaxCells + 1 - LeafNodeRightSplitCount
)

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
	p.setNodeRoot(false)
	*p.leafNodeNumCells() = 0
}

func (p *Page) printLeafNode() {
	numCells := *p.leafNodeNumCells()
	fmt.Printf("leaf (size %d)\n", numCells)
	for i := uint32_t(0); i < numCells; i++ {
		key := *p.leafNodeKey(i)
		fmt.Printf("  - %d : %d\n", i, key)
	}
}

func (c *Cursor) leafNodeSplitAndInsert(key uint32_t, value *Row) {
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
			oldPage.leafNodeCell(uint32_t(i - 1)).moveTo(destinationCell)
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

func (t *Table) createNewRoot(rightChildPageNum uint32_t) {
	root := t.pager.getPage(t.rootPageNum)
	//rightChild := t.pager.getPage(rightChildPageNum)
	leftChildPageNum := t.pager.getUnusedPageNum()
	leftChild := t.pager.getPage(leftChildPageNum)

	copy((*[PageSize]byte)(unsafe.Pointer(leftChild))[:], (*[PageSize]byte)(unsafe.Pointer(root))[:])
	leftChild.setNodeRoot(false)

	internalNode := new(InternalPage)
	internalNode.initializeInternalNode()
	internalNode.setNodeRoot(true)
	*(internalNode.internalNodeNumKeys()) = 1
	*(internalNode.internalNodeChild(0)) = leftChildPageNum
	leftChildMaxKey := leftChild.getMaxKey()
	*(internalNode.internalNodeKey(0)) = leftChildMaxKey
	*(internalNode.internalNodeRightChild()) = rightChildPageNum
}

func (p *InternalPage) internalNodeNumKeys() *uint32_t {
	if p.pageType == PageInternal {
		return &(p.numCells)
	}
	panic("trying to get numCells from internal node")
}

func (p *InternalPage) internalNodeRightChild() *uint32_t {
	return &p.rightChild
}

func (p *InternalPage) internalNodeCell(cellNum uint32_t) *InternalNodeCell {
	return &(p.cells[cellNum])
}

func (p *InternalPage) internalNodeChild(childNum uint32_t) *uint32_t {
	numKeys := p.numCells
	if childNum > numKeys {
		fmt.Printf("Tried to access child_num %d > num_keys %d\n", childNum, numKeys)
		os.Exit(ExitFailure)
	} else if childNum == numKeys {
		return p.internalNodeRightChild()
	}
	return &p.internalNodeCell(childNum).value
}

func (p *InternalPage) internalNodeKey(keyNum uint32_t) *uint32_t {
	return &(p.cells[keyNum].key)
}

func (p *InternalPage) isNodeRoot() bool {
	return p.isRoot == 1
}

func (p *InternalPage) setNodeRoot(isRoot bool) { //考虑用interface实现公用功能
	if isRoot {
		p.isRoot = 1
	} else {
		p.isRoot = 0
	}
}

func (p *Page) setNodeRoot(isRoot bool) {
	if isRoot {
		p.isRoot = 1
	} else {
		p.isRoot = 0
	}
}

func (p *InternalPage) initializeInternalNode() {
	p.setPageType(PageInternal)
	p.setNodeRoot(false)
	*p.internalNodeNumKeys() = 0
}

func (p *InternalPage) getMaxKey() uint32_t {
	return *(p.internalNodeKey(*(p.internalNodeNumKeys()) - 1))
}

func (p *Page) getMaxKey() uint32_t {
	return *(p.leafNodeKey(*(p.leafNodeNumCells()) - 1))
}

func indent(level uint32_t)  {
	for i := 0; i < int(level); i++ {
		fmt.Printf(" ")
	}
}

func (p *Pager) printTree(pagNum, indentationLevel uint32_t) {
	return
}

//func (c *Cursor) internalNodeFind(pageNum, key uint32_t)  {
//	node := c.table.pager.getPage(pageNum)
//	numKeys :=
//}
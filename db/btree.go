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

func (p *LeafPage) leafNodeNumCells() *uint32_t {
	if p.header.pageType == PageLeaf {
		return &(p.header.numCells)
	}
	panic("trying to get numCells from internal node")
}

func (p *LeafPage) leafNodeCell(cellNum uint32_t) *LeafPageCell {
	return &(p.body.cells[cellNum])
}

func (p *LeafPage) leafNodeKey(cellNum uint32_t) *uint32_t {
	return &(p.leafNodeCell(cellNum).key)
}

func (p *LeafPage) leafNodeValue(cellNum uint32_t) *Row {
	return &(p.body.cells[cellNum].value)
}

func (p *LeafPage) initializeLeafNode() {
	p.setPageType(PageLeaf) // actually, it's not necessary since default PageType is PageLeaf
	p.setNodeRoot(false)
	*p.leafNodeNumCells() = 0
}

func (p *LeafPage) printLeafNode() {
	numCells := *p.leafNodeNumCells()
	fmt.Printf("leaf (size %d)\n", numCells)
	for i := uint32_t(0); i < numCells; i++ {
		key := *p.leafNodeKey(i)
		fmt.Printf("  - %d : %d\n", i, key)
	}
}

func (c *Cursor) leafNodeSplitAndInsert(key uint32_t, value *Row) {
	oldHeader, oldBody := c.table.pager.getPage(c.pageNum)
	oldPage := &LeafPage{header: oldHeader, body: (*LeafPageBody)(oldBody)}
	newPageNum := c.table.pager.getUnusedPageNum()
	newHeader, newBody := c.table.pager.getPage(newPageNum)
	newPage := &LeafPage{header: newHeader, body: (*LeafPageBody)(newBody)}
	newPage.initializeLeafNode()

	// TODO:有待改进
	for i := int32(LeafNodeMaxCells); i >= 0; i-- {
		var destinationPage *LeafPage
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

	if oldPage.header.isRoot != 0 {
		c.table.createNewRoot(newPageNum)
	} else {
		fmt.Println("Need to implement updating parent after split.")
		os.Exit(ExitFailure)
	}
}

func (p *Pager) getUnusedPageNum() uint32_t { return p.numPages }

func (t *Table) createNewRoot(rightChildPageNum uint32_t) {
	rootHeader, rootBody := t.pager.getPage(t.rootPageNum)
	//root := LeafPage{header: rootHeader, body: (*LeafPageBody)(rootBody)}
	//rightChild := t.pager.getPage(rightChildPageNum)
	leftChildPageNum := t.pager.getUnusedPageNum()
	leftChildHeader, leftChildBody := t.pager.getPage(leftChildPageNum)
	leftChild := LeafPage{header: leftChildHeader, body: (*LeafPageBody)(leftChildBody)}

	copy((*[PageHeaderSize]byte)(unsafe.Pointer(leftChildHeader))[:],
		(*[PageHeaderSize]byte)(unsafe.Pointer(rootHeader))[:])
	copy((*[PageBodySize]byte)(leftChildBody)[:], (*[PageBodySize]byte)(rootBody)[:])
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
	if p.header.pageType == PageInternal {
		return &(p.header.numCells)
	}
	panic("trying to get numCells from internal node")
}

func (p *InternalPage) internalNodeRightChild() *uint32_t {
	return &p.body.rightChild
}

func (p *InternalPage) internalNodeCell(cellNum uint32_t) *uint32_t {
	return &(p.body.cells[cellNum].value)
}

func (p *InternalPage) internalNodeChild(childNum uint32_t) *uint32_t {
	numKeys := p.header.numCells
	if childNum > numKeys {
		fmt.Printf("Tried to access child_num %d > num_keys %d\n", childNum, numKeys)
		os.Exit(ExitFailure)
	} else if childNum == numKeys {
		return p.internalNodeRightChild()
	}
	return p.internalNodeCell(childNum)
}

func (p *InternalPage) internalNodeKey(keyNum uint32_t) *uint32_t {
	return &(p.body.cells[keyNum].key)
}

func (p *InternalPage) isNodeRoot() bool {
	return p.header.isRoot == 1
}

func (p *LeafPage) setNodeRoot(isRoot bool) {
	if isRoot {
		p.header.isRoot = 1
	} else {
		p.header.isRoot = 0
	}
}

func (p *InternalPage) setNodeRoot(isRoot bool) {
	if isRoot {
		p.header.isRoot = 1
	} else {
		p.header.isRoot = 0
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

func (p *LeafPage) getMaxKey() uint32_t {
	return *(p.leafNodeKey(*(p.leafNodeNumCells()) - 1))
}

func indent(level uint32_t)  {
	for i := 0; i < int(level); i++ {
		fmt.Printf(" ")
	}
}

func (p *Pager) printTree(pageNum, indentationLevel uint32_t) {
	header, body := p.getPage(pageNum)

	switch header.pageType {
	case PageLeaf:
		leafPage := LeafPage{header: header, body: (*LeafPageBody)(body)}
		numKeys := *leafPage.leafNodeNumCells()
		indent(indentationLevel)
		fmt.Printf("- leaf (size %d)\n", numKeys)
		for i:=uint32_t(0); i<numKeys;i++ {
			indent(indentationLevel+1)
			fmt.Printf("- %d\n", *leafPage.leafNodeKey(i))
		}
	case PageInternal:
		internalPage := InternalPage{header: header, body: (*InternalPageBody)(body)}
		numKeys := *internalPage.internalNodeNumKeys()
		indent(indentationLevel)
		fmt.Printf("- internal (size %d)\n", numKeys)
		for i:=uint32_t(0); i<numKeys;i++ {
			child := *internalPage.internalNodeChild(i)
			p.printTree(child, indentationLevel+1)
			fmt.Printf("- key %d\n", *internalPage.internalNodeKey(i))
		}
		child := *internalPage.internalNodeRightChild()
		p.printTree(child, indentationLevel+1)
	}
}

func (t *Table) internalNodeFind(pageNum, key uint32_t) *Cursor {
	header, body := t.pager.getPage(pageNum)
	internalPage := InternalPage{header: header, body: (*InternalPageBody)(body)}
	numKeys := *internalPage.internalNodeNumKeys()

	minIndex, maxIndex := uint32_t(0), numKeys
	for minIndex != maxIndex {
		index := (minIndex + maxIndex) / 2
		keyToRight := *internalPage.internalNodeKey(index)
		if keyToRight >= key {
			maxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	childNum := *internalPage.internalNodeCell(minIndex)
	childHeader, _ := t.pager.getPage(childNum)
	switch childHeader.pageType {
	case PageLeaf:
		return t.leafNodeFind(childNum, key)
	default:
		return t.internalNodeFind(childNum, key)
	}
}
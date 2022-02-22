package db

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

type uint32_t uint32

const (
	ExitSuccess int = iota
	ExitFailure
	ColumnUsernameSize int = 32
	ColumnEmailSize    int = 255
)

type MetaCommandResult int

const (
	MetaCommandSuccess MetaCommandResult = iota
	MetaCommandUnrecognizedCommand
)

type StatementType int

const (
	StatementInsert StatementType = iota
	StatementSelect
)

type PrepareResult int

const (
	PrepareSuccess PrepareResult = iota
	PrepareSyntaxError
	PrepareStringTooLong
	PrepareNegativeId
	PrepareUnrecognizedStatement
)

type Statement struct {
	sType       StatementType
	rowToInsert Row
}

type ExecuteResult int

const (
	ExecuteSuccess ExecuteResult = iota
	ExecuteTableFull
	ExecuteDuplicateKey
	ExecuteStatementTypeUnrecognized
)

type InputBuffer struct {
	buffer []byte
}

const (
	IdSize         = uint32_t(unsafe.Sizeof(Row{}.id))
	UsernameSize   = uint32_t(unsafe.Sizeof(Row{}.username))
	EmailSize      = uint32_t(unsafe.Sizeof(Row{}.email))
	IdOffset       = uint32_t(0)
	UsernameOffset = IdOffset + IdSize
	EmailOffset    = UsernameOffset + UsernameSize
	RowSize        = IdSize + UsernameSize + EmailSize

	TableMaxPages uint32_t = 100
	PageSize      uint32_t = 4096
)

type Table struct {
	rootPageNum uint32_t
	pager       *Pager
}

type Pager struct {
	fileDescriptor *os.File
	fileLength     uint32_t
	numPages       uint32_t
	headers        [TableMaxPages]*PageHeader
	bodies         [TableMaxPages]unsafe.Pointer
}

type PageHeader struct {
	pageType      PageType
	isRoot        uint8_t
	parentPointer uint32_t
	numCells      uint32_t
}

const (
	PageHeaderSize = uint32_t(unsafe.Sizeof(PageHeader{}))
	PageBodySize   = PageSize - PageHeaderSize
)

//type Page struct { // to create a pagesize memory without malloc()
//	header PageHeader
//	body   [PageBodySize]byte
//}

type LeafPage struct {
	header *PageHeader
	body   *LeafPageBody
}

type InternalPage struct {
	header *PageHeader
	body   *InternalPageBody
}

type LeafPageBody struct {
	nextLeaf uint32_t
	cells    [LeafNodeMaxCells]LeafPageCell
}

type InternalPageBody struct {
	rightChild uint32_t
	cells      [InternalNodeMaxCells]InternalPageCell
}

type Row struct {
	id       uint32_t
	username [ColumnUsernameSize]byte
	email    [ColumnEmailSize]byte
}

type LeafPageCell struct {
	key   uint32_t
	value Row
}

type InternalPageCell struct {
	value uint32_t
	key   uint32_t
}

type Cursor struct {
	table      *Table
	pageNum    uint32_t
	cellNum    uint32_t
	endOfTable bool
}

func dbOpen(fileName *string) *Table {
	pager := pagerOpen(fileName)

	table := new(Table)
	table.pager = pager
	table.rootPageNum = 0
	if pager.numPages == 0 {
		header, body := pager.getPage(0)
		leafPage := LeafPage{header: header, body: (*LeafPageBody)(body)}
		leafPage.initializeLeafNode()
		leafPage.setNodeRoot(true)
	}
	return table
}

func printConstants() {
	fmt.Printf("ROW_SIZE: %d\n", RowSize)
	fmt.Printf("COMMON_NODE_HEADER_SIZE: %d\n", CommonNodeHeaderSize)
	fmt.Printf("LEAF_NODE_HEADER_SIZE: %d\n", LeafNodeHeaderSize)
	fmt.Printf("LEAF_NODE_CELL_SIZE: %d\n", LeafNodeCellSize)
	fmt.Printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LeafNodeSpaceForCells)
	fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", LeafNodeMaxCells)
}

func (c *LeafPageCell) moveTo(dest *LeafPageCell) {
	copy(((*[LeafNodeCellSize]byte)(unsafe.Pointer(dest)))[:],
		((*[LeafNodeCellSize]byte)(unsafe.Pointer(c)))[:])
}

func pagerOpen(fileName *string) *Pager {
	fd, err := os.OpenFile(*fileName, os.O_RDWR|os.O_CREATE, 0755) //fd实际为file指针，文件描述符用*File.fd()获取
	if err != nil {
		fmt.Println("Unable to open file.")
		os.Exit(ExitFailure)
	}
	fileInfo, err := fd.Stat()
	if err != nil {
		fmt.Println("Unable to get file info.")
		os.Exit(ExitFailure)
	}
	fileLength := fileInfo.Size()

	pager := new(Pager)
	pager.fileDescriptor = fd
	pager.fileLength = uint32_t(fileLength)
	pager.numPages = uint32_t(fileLength) / PageSize
	if uint32_t(fileLength)%PageSize != 0 {
		fmt.Println("Db file is not a whole number of pages. Corrupt file.")
		os.Exit(ExitFailure)
	}

	return pager
}

func (row Row) printRow() {
	fmt.Printf("(%d, %s, %s)\n", row.id, row.username, row.email)
}

func (row *Row) serializeRow(destination unsafe.Pointer) {
	copy(((*[RowSize]byte)(destination))[:],
		((*[RowSize]byte)(unsafe.Pointer(row)))[:])
}

func (row *Row) deSerializeRow(source unsafe.Pointer) {
	copy(((*[RowSize]byte)(unsafe.Pointer(row)))[:],
		((*[RowSize]byte)(source))[:])
}

func (c *Cursor) cursorValue() *Row {
	pageNum := c.pageNum
	header, body := c.table.pager.getPage(pageNum)
	leafPage := LeafPage{header: header, body: (*LeafPageBody)(body)}
	return leafPage.leafNodeValue(c.cellNum)
}

func (p *Pager) getPage(pageNum uint32_t) (*PageHeader, unsafe.Pointer) {
	if pageNum > TableMaxPages {
		fmt.Printf("Tried to fetch page number out of bounds. %d > %d\n", pageNum, TableMaxPages)
		os.Exit(ExitFailure)
	}
	if p.headers[pageNum] == nil {
		header, bodyArr := p._getPage(pageNum)
		p.headers[pageNum] = header
		//leafBody := (*LeafPageBody)(unsafe.Pointer(bodyArr))
		p.bodies[pageNum] = unsafe.Pointer(bodyArr)
	}
	return p.headers[pageNum], p.bodies[pageNum]
}

func (p *Pager) _getPage(pageNum uint32_t) (*PageHeader, *[PageBodySize]byte) {
	numPages := p.fileLength / PageSize
	header := new(PageHeader)
	var b [PageSize]byte
	var bodyRawArr [PageBodySize]byte
	if p.fileLength%PageSize != 0 {
		numPages += 1
	}
	if pageNum < numPages {
		_, err := p.fileDescriptor.Seek(int64(pageNum*PageSize), 0)
		if err != nil {
			fmt.Println("Error occurred while moving ptr.")
			os.Exit(ExitFailure)
		}

		_, err = p.fileDescriptor.Read(b[:])
		if err != nil {
			fmt.Printf("\"Error reading file: %s\n", err)
			os.Exit(ExitFailure)
		}
		copy(((*[PageHeaderSize]byte)(unsafe.Pointer(header)))[:], b[:])
		copy(bodyRawArr[:], b[PageHeaderSize:])
	}
	//p.bodies[pageNum] = unsafe.Pointer(&bodyRawArr)
	if pageNum >= p.numPages {
		p.numPages = pageNum + 1
	}
	return header, &bodyRawArr
}

func (t *Table) dbClose() {
	p := t.pager

	for i := uint32_t(0); i < p.numPages; i++ {
		if p.headers[i] == nil {
			continue
		}
		p.flush(i)
		p.headers[i] = nil
		p.bodies[i] = nil
	}

	err := p.fileDescriptor.Close()
	if err != nil {
		fmt.Println("Error closing db file.")
		os.Exit(ExitFailure)
	}
	for i := uint32_t(0); i < TableMaxPages; i++ {
		if p.headers[i] != nil {
			p.headers[i] = nil
			p.bodies[i] = nil
		}
	}
	t.free()
}

func (p *Pager) flush(pageNum uint32_t) {
	if p.headers[pageNum] == nil {
		fmt.Printf("Tried to flush null page.\n")
		os.Exit(ExitFailure)
	}
	_, err := p.fileDescriptor.Seek(int64(pageNum*PageSize), 0)
	if err != nil {
		fmt.Printf("Error seeking: %s\n", err)
		os.Exit(ExitFailure)
	}

	pageBytesSlice := (*[PageHeaderSize]byte)(unsafe.Pointer(p.headers[pageNum]))[:]
	pageBytesSlice = append(pageBytesSlice, (*[PageBodySize]byte)(p.bodies[pageNum])[:]...)
	_, err = p.fileDescriptor.Write(pageBytesSlice)
	if err != nil {
		fmt.Printf("Error writing: %s\n", err)
		os.Exit(ExitFailure)
	}
}

func (t *Table) tableStart() *Cursor {
	cursor := t.find(0)
	header, body := t.pager.getPage(cursor.pageNum)
	leafPage := LeafPage{header: header, body: (*LeafPageBody)(body)}
	numCells := *leafPage.leafNodeNumCells()
	cursor.endOfTable = numCells == 0

	return cursor
}

func (c *Cursor) advance() {
	pageNum := c.pageNum
	header, body := c.table.pager.getPage(pageNum)
	leafPage := LeafPage{header: header, body: (*LeafPageBody)(body)}
	c.cellNum += 1
	if c.cellNum >= header.numCells {
		nextPageNum := *leafPage.leafNodeNextLeaf()
		if nextPageNum == 0 {
			c.endOfTable = true
		} else {
			c.pageNum = nextPageNum
			c.cellNum = 0
		}
	}
}

func newInputBuffer() *InputBuffer {
	return new(InputBuffer)
}

func printPrompt() {
	fmt.Print("db > ")
}

func readInput(inputBuffer *InputBuffer) {
	inputReader := bufio.NewReader(os.Stdin)
	input, err := inputReader.ReadBytes('\n')
	if err != nil {
		fmt.Printf("ReadInput with err: %s\n", err)
	}
	inputBuffer.buffer = input
}

func (i *InputBuffer) free() {
	fmt.Println("Buffer Freed.")
	return
}

func (t *Table) free() {
	fmt.Println("Table freed.")
}

func doMetaCommand(inputBuffer *InputBuffer, table *Table) MetaCommandResult {
	if strings.TrimSpace(string(inputBuffer.buffer)) == ".exit" {
		table.dbClose()
		os.Exit(ExitSuccess)
	} else if strings.TrimSpace(string(inputBuffer.buffer)) == ".constants" {
		fmt.Println("Constants:")
		printConstants()
		return MetaCommandSuccess
	} else if strings.TrimSpace(string(inputBuffer.buffer)) == ".btree" {
		fmt.Println("Tree:")
		//header, body := table.pager.getPage(0)
		//leafPage := LeafPage{header: header, body: (*LeafPageBody)(body)}
		//leafPage.printLeafNode()
		table.pager.printTree(0, 0)
		return MetaCommandSuccess
	}
	return MetaCommandUnrecognizedCommand
}

func prepareStatement(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	bufferContent := strings.TrimSpace(string(inputBuffer.buffer))
	if len(bufferContent) >= 6 {
		switch bufferContent[:6] {
		case "insert":
			return prepareInsert(&bufferContent, statement)
		case "select":
			statement.sType = StatementSelect
			return PrepareSuccess
		}
	}
	return PrepareUnrecognizedStatement
}

func prepareInsert(buffer *string, statement *Statement) PrepareResult {
	var username string
	var email string

	statement.sType = StatementInsert
	splitSlice := strings.Split(*buffer, " ")

	if len(splitSlice) != 4 {
		return PrepareSyntaxError
	}
	id, err := strconv.Atoi(splitSlice[1])
	if err != nil {
		return PrepareSyntaxError
	}
	if id < 0 {
		return PrepareNegativeId
	}

	username = splitSlice[2]
	email = splitSlice[3]
	if len(username) > ColumnUsernameSize || len(email) > ColumnEmailSize {
		return PrepareStringTooLong
	}

	statement.rowToInsert.id = uint32_t(uint32(id))
	copy(statement.rowToInsert.username[:], username)
	copy(statement.rowToInsert.email[:], email)

	return PrepareSuccess
}

func executeStatement(statement *Statement, table *Table) ExecuteResult {
	switch statement.sType {
	case StatementInsert:
		return statement.executeInsert(table)
	case StatementSelect:
		return statement.executeSelect(table)
	}
	return ExecuteStatementTypeUnrecognized
}

func (s *Statement) executeInsert(table *Table) ExecuteResult {
	header, body := table.pager.getPage(table.rootPageNum)
	leafPage := LeafPage{header: header, body: (*LeafPageBody)(body)}
	numCells := *(leafPage.leafNodeNumCells())

	rowToInsert := s.rowToInsert
	keyToInsert := rowToInsert.id
	cursor := table.find(keyToInsert)

	if cursor.cellNum < numCells {
		keyAtIndex := *leafPage.leafNodeKey(cursor.cellNum)
		if keyAtIndex == keyToInsert {
			return ExecuteDuplicateKey
		}
	}
	cursor.leafNodeInsert(rowToInsert.id, &rowToInsert)

	return ExecuteSuccess
}

func (c *Cursor) leafNodeInsert(key uint32_t, value *Row) {
	header, body := c.table.pager.getPage(c.pageNum)
	leafPage := LeafPage{header: header, body: (*LeafPageBody)(body)}
	numCells := *leafPage.leafNodeNumCells()
	if numCells >= LeafNodeMaxCells {
		//fmt.Println("Need to implement splitting a leaf node.")
		//os.Exit(ExitFailure)
		c.leafNodeSplitAndInsert(key, value)
		return
	}

	if c.cellNum < numCells {
		for i := numCells; i > c.cellNum; i-- {
			leafPage.leafNodeCell(i - 1).moveTo(leafPage.leafNodeCell(i))
		}
	}
	*(leafPage.leafNodeNumCells()) += 1
	*(leafPage.leafNodeKey(c.cellNum)) = key
	value.serializeRow(unsafe.Pointer(leafPage.leafNodeValue(c.cellNum)))
}

func (t *Table) find(key uint32_t) *Cursor {
	rootPageNum := t.rootPageNum
	header, _ := t.pager.getPage(rootPageNum)

	if header.pageType == PageLeaf {
		return t.leafNodeFind(rootPageNum, key)
	} else {
		//fmt.Println("Need to implement searching an internal node.")
		//os.Exit(ExitFailure)
		//return t.internalNodeFind(rootPageNum, key)
		panic("Need to implement searching an internal node.")
	}
}

func (p *LeafPage) getPageType() PageType {
	return p.header.pageType
}

func (p *LeafPage) setPageType(t PageType) {
	p.header.pageType = t
}

func (p *InternalPage) getPageType() PageType {
	return p.header.pageType
}

func (p *InternalPage) setPageType(t PageType) {
	p.header.pageType = t
}

func (t *Table) leafNodeFind(pageNum, key uint32_t) *Cursor {
	header, body := t.pager.getPage(pageNum)
	leafPage := LeafPage{header: header, body: (*LeafPageBody)(body)}
	numCells := *(leafPage.leafNodeNumCells())

	cursor := Cursor{table: t, pageNum: pageNum}
	// Binary search
	minIndex := uint32_t(0)
	onePastMaxIndex := numCells
	for onePastMaxIndex != minIndex {
		index := (minIndex + onePastMaxIndex) / 2
		keyAtIndex := *(leafPage.leafNodeKey(index))
		if key == keyAtIndex {
			cursor.cellNum = index
			return &cursor
		}
		if key < keyAtIndex {
			onePastMaxIndex = index
		} else {
			minIndex = index + 1
		}
	}
	cursor.cellNum = minIndex
	return &cursor
}

func (s *Statement) executeSelect(table *Table) ExecuteResult {
	var row Row
	for c := table.tableStart(); !c.endOfTable; c.advance() {
		row.deSerializeRow(unsafe.Pointer(c.cursorValue()))
		row.printRow()
	}
	return ExecuteSuccess
}

func Run(db string) int {
	if db == "" {
		fmt.Printf("Must supply a database filename.\n")
		os.Exit(ExitFailure)
	}
	inputBuffer := newInputBuffer()
	table := dbOpen(&db)

	for {
		printPrompt()
		readInput(inputBuffer)

		if inputBuffer.buffer[0] == '.' {
			switch doMetaCommand(inputBuffer, table) {
			case MetaCommandSuccess:
				continue
			case MetaCommandUnrecognizedCommand:
				fmt.Printf("Unrecognized command '%s'\n", inputBuffer.buffer)
				continue
			}
		}

		var statement Statement

		switch prepareStatement(inputBuffer, &statement) {
		case PrepareSuccess:
		case PrepareSyntaxError:
			fmt.Printf("Syntax error. Could not parse statement.\n")
			continue
		case PrepareStringTooLong:
			fmt.Println(" String is too long.")
			continue
		case PrepareNegativeId:
			fmt.Println("ID must be positive.")
			continue
		case PrepareUnrecognizedStatement:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n",
				strings.TrimSpace(string(inputBuffer.buffer)))
			continue
		}

		switch executeStatement(&statement, table) {
		case ExecuteSuccess:
			fmt.Println("Executed.")
		case ExecuteTableFull:
			fmt.Println("Error: Table full.")
		case ExecuteDuplicateKey:
			fmt.Println("Error: Duplicate key.")
		}
	}
}

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
	ExitSuccess        int = iota
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
	ExecuteStatementTypeUnrecognized
)

type InputBuffer struct {
	buffer            []byte
}

const (
	IdSize = uint32_t(unsafe.Sizeof(Row{}.id))
	UsernameSize = uint32_t(unsafe.Sizeof(Row{}.username))
	EmailSize = uint32_t(unsafe.Sizeof(Row{}.email))
	IdOffset = uint32_t(0)
	UsernameOffset = IdOffset + IdSize
	EmailOffset = UsernameOffset + UsernameSize
	RowSize = IdSize + UsernameSize + EmailSize

	TableMaxPages uint32_t = 100
	PageSize      uint32_t = 4096
	RowsPerPage = PageSize / RowSize
	TableMaxRows = TableMaxPages * RowsPerPage
)

type Table struct {
	numRows uint32_t
	pager *Pager
}

type Pager struct {
	fileDescriptor *os.File
	fileLength uint32_t
	pages   [TableMaxPages]*Page
}

type Page struct {  // to create a pagesize memory without malloc()
	numRows uint32_t
	rows [RowsPerPage]Row
}

type Row struct {
	id       uint32                   `name:"id"`
	username [ColumnUsernameSize]byte `name:"username"`
	email    [ColumnEmailSize]byte    `name:"email"`
}

func dbOpen(fileName *string)*Table{
	pager := pagerOpen(fileName)
	numRows := pager.fileLength / RowSize

	table := new(Table)
	table.pager = pager
	table.numRows = numRows
	return table
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

	//for i := uint32_t(0); i < TableMaxPages; i++{
	//	pager.pages[i] = nil
	//}
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

func (table *Table) rowSlot(rowNum uint32_t) uintptr {
	pageNum := rowNum / RowsPerPage
	page := table.pager.getPage(pageNum)

	rowOffset := rowNum % RowsPerPage
	byteOffset := rowOffset * RowSize
	return uintptr(unsafe.Pointer(page)) + uintptr(byteOffset)
}

func (p *Pager) getPage(pageNum uint32_t) *Page {
	if pageNum > TableMaxPages {
		fmt.Printf("Tried to fetch page number out of bounds. %d > %d\n", pageNum, TableMaxPages)
		os.Exit(ExitFailure)
	}
	if p.pages[pageNum] == nil {
		page := new(Page)
		numPages := p.fileLength / PageSize

		if p.fileLength % PageSize != 0 {
			numPages += 1
		}
		if pageNum<numPages {
			_, err := p.fileDescriptor.Seek(int64(pageNum*PageSize), 0)
			if err != nil {
				fmt.Println("Error occurred while moving ptr.")
				os.Exit(ExitFailure)
			}
			b := make([]byte, PageSize)
			_, err = p.fileDescriptor.Read(b)
			if err != nil {
				fmt.Printf("\"Error reading file: %s\n", err)
				os.Exit(ExitFailure)
			}
			copy(((*[PageSize]byte)(unsafe.Pointer(page)))[:], b)
		}
		p.pages[pageNum] = page
	}
	return p.pages[pageNum]
}

func (table *Table) dbClose()  {
	p := table.pager
	numFullPages := table.numRows / RowsPerPage

	for i:=uint32_t(0); i < numFullPages; i++{
		if p.pages[i] == nil {
			continue
		}
		p.flush(i,PageSize)
		p.pages[i] = nil
	}

	numAdditionalRows := table.numRows%RowsPerPage
	if numAdditionalRows>0{
		pageNum := numFullPages
		if p.pages[pageNum] != nil {
			p.flush(pageNum, numAdditionalRows*RowSize)
			p.pages[pageNum] = nil
		}
	}
	err := p.fileDescriptor.Close()
	if err != nil {
		fmt.Println("Error closing db file.")
		os.Exit(ExitFailure)
	}
	for i := uint32_t(0); i < TableMaxPages; i++{
		page := p.pages[i]
		if page != nil {p.pages[i]=nil}
	}
	//p.free()
	table.free()
}

func (p *Pager) flush(pageNum, size uint32_t)  {
	if p.pages[pageNum] == nil {
		fmt.Printf("Tried to flush null page.\n")
		os.Exit(ExitFailure)
	}
	_, err := p.fileDescriptor.Seek(int64(pageNum*PageSize), 0)
	if err != nil {
		fmt.Printf("Error seeking: %s\n", err)
		os.Exit(ExitFailure)
	}
	pageBytesSlice := (*[PageSize]byte)(unsafe.Pointer(p.pages[pageNum]))[:size]
	_, err = p.fileDescriptor.Write(pageBytesSlice)
	if err != nil {
		fmt.Printf("Error writing: %s\n", err)
		os.Exit(ExitFailure)
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

func (inputBuffer *InputBuffer) free() {
	fmt.Println("Buffer Freed.")
	return
}

func (table *Table) free(){
	fmt.Println("Table freed.")
}

func doMetaCommand(inputBuffer *InputBuffer, table *Table) MetaCommandResult {
	if strings.TrimSpace(string(inputBuffer.buffer))[:5] == ".exit" {
		table.dbClose()
		os.Exit(ExitSuccess)
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
	if len(splitSlice) != 4 {return PrepareSyntaxError}
	id , err := strconv.Atoi(splitSlice[1])
	if err != nil {return PrepareSyntaxError}
	if id < 0 {return PrepareNegativeId}
	username = splitSlice[2]
	email = splitSlice[3]
	if len(username) > ColumnUsernameSize || len(email) > ColumnEmailSize {
		return PrepareStringTooLong
	}
	statement.rowToInsert.id = uint32(id)
	copy(statement.rowToInsert.username[:], username)
	copy(statement.rowToInsert.email[:], email)
	return PrepareSuccess
}

func executeStatement(statement *Statement, table *Table) ExecuteResult{
	switch statement.sType {
	case StatementInsert:
		return statement.executeInsert(table)
	case StatementSelect:
		return statement.executeSelect(table)
	}
	return ExecuteStatementTypeUnrecognized
}

func (statement *Statement) executeInsert(table *Table) ExecuteResult {
	if table.numRows >= TableMaxRows{
		return ExecuteTableFull
	}
	rowToInsert := statement.rowToInsert
	rowToInsert.serializeRow(unsafe.Pointer(table.rowSlot(table.numRows)))
	table.numRows += 1

	return ExecuteSuccess
}

func (statement *Statement) executeSelect(table *Table) ExecuteResult {
	var row Row
	for i := uint32_t(0); i< table.numRows; i++ {
		row.deSerializeRow(unsafe.Pointer(table.rowSlot(i)))
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
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", inputBuffer.buffer)
			continue
		}

		switch executeStatement(&statement, table) {
		case ExecuteSuccess:
			fmt.Println("Executed.")
		case ExecuteTableFull:
			fmt.Println("Error: Table full.")
		}
	}
}
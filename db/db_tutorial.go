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
	ExitSuccess        int = 0
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
	pages   [TableMaxPages]*Page
}

type Page struct {
	numRows uint32_t
	rows [RowsPerPage]Row
}

type Row struct {
	id       uint32                   `name:"id"`
	username [ColumnUsernameSize]byte `name:"username"`
	email    [ColumnEmailSize]byte    `name:"email"`
}

func newTable() *Table {
	return new(Table)
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
	page := table.pages[pageNum]
	if page == nil {
		page = new(Page)
		table.pages[pageNum] = page
	}
	rowOffset := rowNum % RowsPerPage
	byteOffset := rowOffset * RowSize
	return uintptr(unsafe.Pointer(page)) + uintptr(byteOffset)
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
		inputBuffer.free()
		table.free()
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
	inputBuffer := newInputBuffer()
	var table = newTable()
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
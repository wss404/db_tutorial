package db

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"unsafe"
)

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
)

type Row struct {
	id       uint32                   `name:"id"`
	username [ColumnUsernameSize]byte `name:"username"`
	email    [ColumnEmailSize]byte    `name:"email"`
}
type size_t uint
type ssize_t int
type uint32_t uint32

type InputBuffer struct {
	buffer            []byte
	bufferLength      size_t
	inputBufferLength ssize_t
}

//func sizeOfAttribute(Struct interface{}, Attribute string) uint32_t {
//	return unsafe.Sizeof(reflect.TypeOf(Struct).FieldByName(Attribute))
//}

var IdSize = uint32_t(unsafe.Sizeof(Row{}.id))
var UsernameSize = uint32_t(unsafe.Sizeof(Row{}.username))
var EmailSize = uint32_t(unsafe.Sizeof(Row{}.email))
var IdOffset = uint32_t(0)
var UsernameOffset = IdOffset + IdSize
var EmailOffset = UsernameOffset + UsernameSize
var RowSize = IdSize + UsernameSize + EmailSize

const (
	TableMaxPages uint32_t = 100
	PageSize      uint32_t = 4096
)

var RowsPerPage = PageSize / uint32_t(RowSize)
var TableMaxRows = TableMaxPages * RowsPerPage

type Table struct {
	numRows uint32_t
	pages   [TableMaxPages]uintptr
}

func (row Row) printRow() {
	fmt.Printf("%d %s %s", row.id, row.username, row.email)
}

//func (row *Row) serializeRow(destination unsafe.Pointer) {
//	memcpy(destination+IdOffset, unsafe.Pointer(&row.id), uint(IdSize))
//	memcpy(destination+UsernameOffset, unsafe.Pointer(&row.username), uint(UsernameOffset))
//	memcpy(destination+EmailOffset, unsafe.Pointer(&row.email), uint(EmailOffset))
//}

func (table *Table) rowSlot(rowNum uint32_t, rowsPerPage int) {
	pageNum := rowNum / RowsPerPage
	page := table.pages[pageNum]
	if page == 0 {
		table.pages[pageNum] = uintptr(unsafe.Pointer([rowsPerPage]Row))
	}
}
//func memcpy(dest unsafe.Pointer, origin unsafe.Pointer, n uint) {
//	copy(([]byte)(dest), ([n]byte)(origin))
//}

func Run(argc int, argv ...string) int {
	var inputBuffer = newInputBuffer()
	for {
		printPrompt()
		readInput(inputBuffer)
		if inputBuffer.buffer[0] == '.' {
			switch doMetaCommand(inputBuffer) {
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
			break
		case PrepareUnrecognizedStatement:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", inputBuffer.buffer)
			continue
		}
		executeCommand(&statement)
		fmt.Println("Executed.")
	}
}

func newInputBuffer() *InputBuffer {
	return new(InputBuffer)
}

func printPrompt() {
	fmt.Print("db > ")
}

func readInput(inputBuffer *InputBuffer) bool {
	n, err := fmt.Scan(&inputBuffer.buffer)
	if err != nil {
		fmt.Printf("Scan with err: %s\n", err)
	}
	return n != 0
}

func closeInputBuffer(inputBuffer *InputBuffer) int {
	fmt.Println("Buffer Freed!")
	return 0
}

func doMetaCommand(inputBuffer *InputBuffer) MetaCommandResult {
	if strings.TrimSpace(string(inputBuffer.buffer))[:5] == ".exit" {
		os.Exit(ExitSuccess)
	}
	return MetaCommandUnrecognizedCommand
}

func prepareStatement(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	if strings.TrimSpace(string(inputBuffer.buffer))[:6] == "insert" {
		statement.sType = StatementInsert
		return PrepareSuccess
	}
	if strings.TrimSpace(string(inputBuffer.buffer))[:6] == "select" {
		statement.sType = StatementSelect
		return PrepareSuccess
	}
	return PrepareUnrecognizedStatement
}

func executeCommand(statement *Statement) {
	switch statement.sType {
	case StatementInsert:
		fmt.Printf("This is where we would do an insert.\n")
		break
	case StatementSelect:
		fmt.Printf("This is where we would do a select.\n")
		break
	}
}

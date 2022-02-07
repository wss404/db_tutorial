package db

import (
	"fmt"
	"strings"
)

const (
	ExitSuccess int = 0
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
	PrepareUnrecognizedStatement
)

type Statement struct {
	stype StatementType
}

type size_t uint
type ssize_t int

type InputBuffer struct {
	buffer            []byte
	bufferLength      size_t
	inputBufferLength ssize_t
}

func Run(argc int, argv ...string) int {
	var inputBuffer = newInputBuffer()
	for {
		printPromt()
		readInput(inputBuffer)
		// if strings.TrimSpace(string(inputBuffer.buffer))[:5] == ".exit" {
		// 	// fmt.Printf("command: %s\n", inputBuffer.buffer)
		// 	closeInputBuffer(inputBuffer)
		// 	os.Exit(ExitSuccess)
		// } else {
		// 	fmt.Printf("Unrecognized Commands: %s\n", inputBuffer.buffer)
		// 	break
		// }
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
	return 0
}

func newInputBuffer() *InputBuffer {
	return new(InputBuffer)
}

func printPromt() {
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
		return MetaCommandSuccess
	}
	return MetaCommandUnrecognizedCommand
}

func prepareStatement(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	if strings.TrimSpace(string(inputBuffer.buffer))[:6] == "insert" {
		statement.stype = StatementInsert
		return PrepareSuccess
	}
	if strings.TrimSpace(string(inputBuffer.buffer))[:6] == "select" {
		statement.stype = StatementSelect
		return PrepareSuccess
	}
	return PrepareUnrecognizedStatement
}

func executeCommand(statement *Statement) {
	switch statement.stype {
	case StatementInsert:
		fmt.Printf("This is where we would do an insert.\n")
		break
	case StatementSelect:
		fmt.Printf("This is where we would do a select.\n")
		break
	}
}

package db

import (
	"fmt"
	"os"
)

const (
	ExitSuccess int = 0
)

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
		if string((inputBuffer.buffer)[:5]) == ".exit" {
			// fmt.Printf("command: %s\n", inputBuffer.buffer)
			closeInputBuffer(inputBuffer)
			os.Exit(ExitSuccess)
		} else {
			fmt.Printf("Unrecognized Commands: %s\n", inputBuffer.buffer)
			break
		}
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
	n, err := fmt.Scan("%s\n", &inputBuffer.buffer)
	if err != nil {
		fmt.Printf("Scan with err: %s\n", err)
	}
	return n != 0
}

func closeInputBuffer(inputBuffer *InputBuffer) int {
	fmt.Println("Buffer Freed!")
	return 0
}

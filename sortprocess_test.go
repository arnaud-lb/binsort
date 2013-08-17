package main

import (
	"bufio"
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"testing"
)

func TestSortWithMinimalBlockSize(t *testing.T) {

	p := &SortProcess{
		RecordSize:  4,
		KeyEnd:      4,
		BlockSize:   4 * 4,
		TempDir:     os.TempDir(),
		InfileName:  "test/input",
		OutfileName: "test/output",
	}

	testSort(t, p)
}

func TestSortWithSmallBlockSize(t *testing.T) {

	p := &SortProcess{
		RecordSize:  4,
		KeyEnd:      4,
		BlockSize:   1000,
		TempDir:     os.TempDir(),
		InfileName:  "test/input",
		OutfileName: "test/output",
	}

	testSort(t, p)
}

func TestSortWithBlockSizeLargerThanFile(t *testing.T) {

	p := &SortProcess{
		RecordSize:  4,
		KeyStart:    0,
		KeyEnd:      4,
		BlockSize:   100000,
		TempDir:     os.TempDir(),
		InfileName:  "test/input",
		OutfileName: "test/output",
	}

	testSort(t, p)
}

func testSort(t *testing.T, p *SortProcess) {

	os.Remove("test/output")

	err := p.Run()

	if !assert.Nil(t, err) {
		return
	}

	s, err := os.Stat("test/output")

	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, s.Size(), 4000)

	file, err := os.Open("test/output")

	if !assert.Nil(t, err) {
		return
	}

	defer file.Close()

	reader := bufio.NewReader(file)

	var prev []byte = nil
	recordno := 0

	for {

		recordno++

		buf := make([]byte, p.RecordSize)
		n, err := io.ReadFull(reader, buf)

		if n != len(buf) {
			if !assert.Equal(t, io.EOF, err) {
				return
			}
			break
		}

		key := buf[p.KeyStart:p.KeyEnd]

		if prev != nil {
			if bytes.Compare(key, prev) < 0 {
				t.Fatalf("record #%d is disordered", recordno)
			}
		}

		prev = key
	}
}

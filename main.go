package main

import (
	"flag"
	"fmt"
	"github.com/golang/glog"
	"io"
	"os"
)

func usage() {

	usage := `
Binsort is a tool to sort binary files

Usage:

	binsort --size|-s record_size [OPTIONS] infile outfile

Options:

	-s, --size=BYTES
		Record size in bytes (mandatory).

	-o, --offset=BYTES
		Ignore the first bytes in record for sorting. Defaults to 0.

	-l, --length=BYTES
		Use only length bytes for sorting. Defaults to size-offset.

	-b, --block-size=RECORDS
		Sort this number of records at once. This determines the
		maximum number of records kept in memory at any time. Defaults
		to the greatest of 1MB or 4 records. More is faster.

	-T, --temporary-directory=DIR
		Use DIR as temporary directory instead of the system
		default. binsort may need to create files as big as the
		input file.

Examples:

	Sort a file composed of 32-bytes records, by comparing only 8 bytes,
	starting at byte 16 in each record:

	binsort --size 32 --offset 16 --length 8 ./input ./output

`
	io.WriteString(os.Stderr, usage)
}

func main() {

	defer glog.Flush()

	process := &SortProcess{}

	flag.Usage = usage

	flag.IntVar(&process.RecordSize, "size", 0, "record size")
	flag.IntVar(&process.RecordSize, "s", 0, "record size")

	flag.IntVar(&process.KeyStart, "offset", 0, "offset")
	flag.IntVar(&process.KeyStart, "o", 0, "offset")

	keyLen := flag.Int("length", 0, "length")
	flag.IntVar(keyLen, "l", 0, "length")

	blockRecordSize := flag.Int("block-size", 0, "block size, in records")
	flag.IntVar(blockRecordSize, "b", 0, "block size, in records")

	flag.StringVar(&process.TempDir, "temporary-directory", "", "temporary directory")
	flag.StringVar(&process.TempDir, "T", "", "temporary directory")

	flag.Parse()

	if flag.NArg() < 1 {
		argError("Error: Missing infile argument")
	}

	if flag.NArg() < 2 {
		argError("Error: Missing outfile argument")
	}

	process.InfileName = flag.Arg(0)
	process.OutfileName = flag.Arg(1)

	if process.RecordSize <= 0 {
		argError("Error: size is mandatory and must be > 0")
	}

	if process.KeyStart < 0 || *keyLen < 0 {
		argError("Error: offset and length must be >= 0")
	}

	if process.KeyStart+*keyLen > process.RecordSize {
		argError("Error: offset + length > size")
	}

	if *keyLen == 0 {
		*keyLen = process.RecordSize - process.KeyStart
	}

	process.KeyEnd = process.KeyStart + *keyLen

	if process.TempDir == "" {
		process.TempDir = os.TempDir()
	}

	if *blockRecordSize == 0 {
		*blockRecordSize = (1 << 20) / process.RecordSize
	}

	if *blockRecordSize < 4 {
		*blockRecordSize = 4
	}

	process.BlockSize = *blockRecordSize * process.RecordSize

	if err := process.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "binsort: %s\n", err)
		os.Exit(1)
	}
}

func argError(msg string) {
	fmt.Fprintf(os.Stderr, "%s\n", msg)
	usage()
	os.Exit(1)
}

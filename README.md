# binsort

Binsort is a tool to sort files of fixed-length binary records.

Features:

 - Can compare only a slice of records when sorting (i.e. it is possible to sort records by comparing only bytes 8-16 of each record)
 - Uses [external sorting](https://en.wikipedia.org/wiki/External_sorting) to be able to sort files larger than main memory
 - Treats records as abstract arrays of bytes. If the data contains integers, sorting gives expected results if they are encoded with their most significant byte first (i.e. in big endian order)

## Install

    go get github.com/arnaud-lb/binsort

This should leave a `binsort` binary in `$GOPATH/bin/`

## Usage

    binsort --size|-s record_size [OPTIONS] infile outfile

### Options

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

### Examples

Sort a file composed of 32-bytes records, by comparing only 8 bytes,
starting at byte 16 in each record:

    binsort --size 32 --offset 16 --length 8 ./input ./output


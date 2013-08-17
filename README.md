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

See `$GOPATH/bin/binsort --help` or the [usage string](https://github.com/arnaud-lb/binsort/blob/master/main.go)


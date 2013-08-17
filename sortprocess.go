package main

import (
	"bytes"
	"fmt"
	"github.com/golang/glog"
	"io"
	"io/ioutil"
	"os"
	"sort"
)

type SortProcess struct {
	RecordSize  int
	KeyStart    int
	KeyEnd      int
	BlockSize   int
	TempDir     string
	InfileName  string
	OutfileName string
}

func (p *SortProcess) Run() error {

	infile, err := os.Open(p.InfileName)

	if err != nil {
		return err
	}

	defer infile.Close()

	outfile, err := ioutil.TempFile(p.TempDir, "binsort")

	if err != nil {
		return err
	}

	defer outfile.Close()
	defer os.Remove(outfile.Name())

	size, err := p.sortBlocks(infile, outfile)

	if err != nil {
		return err
	}

	infile = outfile

	flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	outfile, err = os.OpenFile(p.OutfileName, flags, 0666)

	if err != nil {
		return err
	}

	defer outfile.Close()

	if err := p.merge(infile, outfile, size); err != nil {
		os.Remove(outfile.Name())
		return err
	}

	return nil
}

func (p *SortProcess) sortBlocks(infile *os.File, outfile *os.File) (n int64, err error) {

	for eof := false; !eof; {

		buf := make([]byte, p.BlockSize)

		nn, err := io.ReadFull(infile, buf)

		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				eof = true
			} else {
				return 0, fmt.Errorf("read failed: %v", err)
			}
		}

		if nn%p.RecordSize != 0 {
			fmt.Fprintf(os.Stderr, "Warning: trailing %v bytes at end of file do not form a full record, ignored\n")
			nn -= nn % p.RecordSize
		}

		buf = buf[:nn]
		n += int64(nn)

		sort.Sort(&sortableRecords{
			buf: buf,
			p:   p,
		})

		nn, err = outfile.Write(buf)

		if nn != len(buf) {
			return 0, fmt.Errorf("write failed: %v", err)
		}
	}

	return n, nil
}

func (p *SortProcess) merge(infile *os.File, outfile *os.File, size int64) (err error) {

	if int64(p.BlockSize) >= size {
		return renameOrCopy(infile.Name(), outfile.Name())
	}

	origInfile := infile

	for mergeBlockSize := int64(p.BlockSize); mergeBlockSize < size; mergeBlockSize *= 2 {

		mergeBlocks := size / mergeBlockSize
		if size%mergeBlockSize > 0 {
			mergeBlocks += 1
		}

		lastOne := mergeBlockSize*2 >= size

		if glog.V(8) {
			glog.Infof("merging %v blocks of %v bytes (last pass: %v)\n", mergeBlocks, mergeBlockSize, lastOne)
		}

		tmpfile := outfile

		if !lastOne {
			tmpfile, err = ioutil.TempFile(p.TempDir, "binsort")
			if err != nil {
				if infile != origInfile {
					infile.Close()
					os.Remove(infile.Name())
				}
				return err
			}
		}

		err := p.mergePass(infile, tmpfile, mergeBlockSize, mergeBlocks)

		if infile != origInfile {
			infile.Close()
			os.Remove(infile.Name())
		}

		if err != nil {
			return err
		}

		infile = tmpfile
	}

	return nil
}

func (p *SortProcess) mergePass(infile *os.File, outfile *os.File, mergeBlockSize, blocks int64) error {

	for nthMerge := int64(0); nthMerge < blocks/2; nthMerge++ {

		err := p.mergeBlocks(infile, outfile, nthMerge, mergeBlockSize)

		if err != nil {
			return err
		}
	}

	if blocks%2 == 1 {

		if glog.V(9) {
			glog.Infof("copying trailing block %v (%v-%v bytes)\n", blocks-1, mergeBlockSize*(blocks-1), mergeBlockSize*blocks)
		}

		_, err := infile.Seek(mergeBlockSize*(blocks-1), os.SEEK_SET)

		if err != nil {
			return err
		}

		_, err = outfile.Seek(mergeBlockSize*(blocks-1), os.SEEK_SET)

		if err != nil {
			return err
		}

		_, err = io.Copy(outfile, infile)

		if err != nil {
			return err
		}
	}

	return nil
}

func (p *SortProcess) mergeBlocks(infile *os.File, outfile *os.File, nthMerge, mergeBlockSize int64) error {

	inputSize := p.BlockSize / 4
	inputSize -= inputSize % p.RecordSize

	outputSize := p.BlockSize - inputSize*2
	outputSize -= outputSize % p.RecordSize

	blocks := []*mergeableBlock{
		&mergeableBlock{
			buf:       make([]byte, inputSize),
			bufOffset: int64(inputSize),
			pos:       nthMerge * 2 * mergeBlockSize,
			size:      mergeBlockSize,
			p:         p,
		},
		&mergeableBlock{
			buf:       make([]byte, inputSize),
			bufOffset: int64(inputSize),
			pos:       nthMerge*2*mergeBlockSize + mergeBlockSize,
			size:      mergeBlockSize,
			p:         p,
		},
	}

	out := make([]byte, outputSize)
	out = out[0:0]
	outOffset := 0

	if glog.V(9) {
		glog.Infof("Merging blocks %v-%v (%v-%v bytes)", nthMerge*2, nthMerge*2+1, blocks[0].pos, blocks[1].pos+mergeBlockSize)
	}

	for len(blocks) > 0 {

		var less *mergeableBlock

		for i := 0; i < len(blocks); {

			b := blocks[i]

			err := b.fill(infile)

			if err != nil {
				if err == io.EOF {
					blocks = append(blocks[0:i], blocks[i+1:len(blocks)]...)
					continue
				} else {
					return err
				}
			}

			if less == nil {
				less = b
			} else if bytes.Compare(b.key(), less.key()) < 0 {
				less = b
			}

			i++
		}

		if less != nil {

			r := less.record()

			out = out[0 : outOffset+p.RecordSize]
			copy(out[outOffset:], r)

			outOffset += p.RecordSize

			less.next()

			if outOffset == cap(out) {

				n, err := outfile.Write(out)

				if n != len(out) || err != nil {
					os.Remove(outfile.Name())
					return err
				}

				outOffset = 0
				out = out[0:0]
			}
		}
	}

	if len(out) > 0 {

		n, err := outfile.Write(out)

		if n != len(out) || err != nil {
			os.Remove(outfile.Name())
			return err
		}
	}

	return nil
}

type mergeableBlock struct {
	buf       []byte
	bufOffset int64
	pos       int64
	posOffset int64
	size      int64
	p         *SortProcess
}

func (b *mergeableBlock) fill(file *os.File) error {

	if b.bufOffset >= int64(len(b.buf)) {

		if b.posOffset+int64(len(b.buf)) > b.size {
			b.buf = b.buf[0 : b.size-b.posOffset]
		}

		var err error
		n := 0

		for n < len(b.buf) && err == nil {
			var nn int
			nn, err = file.ReadAt(b.buf[n:], b.pos+b.posOffset)
			n += nn
			b.posOffset += int64(nn)
		}

		if err != nil && err != io.EOF {
			return err
		}

		n -= n % b.p.RecordSize

		b.buf = b.buf[0:n]
		b.bufOffset = 0
	}

	if len(b.buf) == 0 {
		return io.EOF
	}

	return nil
}

func (b *mergeableBlock) record() []byte {
	return b.buf[b.bufOffset : b.bufOffset+int64(b.p.RecordSize)]
}

func (b *mergeableBlock) key() []byte {
	return b.buf[b.bufOffset+int64(b.p.KeyStart) : b.bufOffset+int64(b.p.KeyEnd)]
}

func (b *mergeableBlock) next() {
	b.bufOffset += int64(b.p.RecordSize)
}

type sortableRecords struct {
	buf []byte
	tmp []byte
	p   *SortProcess
}

func (r *sortableRecords) Len() int {
	return len(r.buf) / r.p.RecordSize
}

func (r *sortableRecords) Less(i, j int) bool {

	recordSize, keyStart, keyEnd := r.p.RecordSize, r.p.KeyStart, r.p.KeyEnd

	ki := r.buf[i*recordSize+keyStart : i*recordSize+keyEnd]
	kj := r.buf[j*recordSize+keyStart : j*recordSize+keyEnd]

	return bytes.Compare(ki, kj) < 0
}

func (r *sortableRecords) Swap(i, j int) {

	recordSize := r.p.RecordSize

	if r.tmp == nil {
		r.tmp = make([]byte, recordSize)
	}

	ri := r.buf[i*recordSize : i*recordSize+recordSize]
	rj := r.buf[j*recordSize : j*recordSize+recordSize]

	copy(r.tmp, ri)
	copy(ri, rj)
	copy(rj, r.tmp)
}

func renameOrCopy(oldname, newname string) error {

	if err := os.Rename(oldname, newname); err == nil {
		return nil
	}

	oldfile, err := os.Open(oldname)

	if err != nil {
		return err
	}

	flags := os.O_CREATE | os.O_TRUNC | os.O_WRONLY
	newfile, err := os.OpenFile(newname, flags, 0666)

	if err != nil {
		return err
	}

	if _, err = io.Copy(newfile, oldfile); err != nil {
		os.Remove(newname)
		return err
	}

	os.Remove(oldname)

	return nil
}

/*
Copyright 2025 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sortio

import (
	"bufio"
	"fmt"
	"io"

	"vitess.io/vitess/go/sqltypes"
)

const arenaBlockSize = 64 * 1024

type (
	// Run represents metadata for a sorted run stored on disk.
	Run struct {
		Offset   int64 // byte offset into the temp file
		Length   int64 // total byte length on disk
		RowCount int   // number of rows in this run
	}

	// TinyWeighter applies a tiny weight to a value for fast comparisons.
	TinyWeighter struct {
		Col   int
		Apply func(v *sqltypes.Value)
	}

	// arena is a simple bump allocator for byte slices. When the current
	// block runs out, a new one is allocated. Old blocks stay alive via
	// references from Values that sub-slice into them.
	arena struct {
		buf []byte
		pos int
	}

	// RunReader reads rows sequentially from a sorted run on disk.
	RunReader struct {
		codec         *RowCodec
		reader        *bufio.Reader
		remaining     int
		tinyWeighters []TinyWeighter
		arena         arena
		hdr           [packetHeaderSize]byte
	}
)

func (a *arena) alloc(n int) []byte {
	if a.pos+n > len(a.buf) {
		a.buf = make([]byte, max(n, arenaBlockSize))
		a.pos = 0
	}
	s := a.buf[a.pos : a.pos+n]
	a.pos += n
	return s
}

// NewRunReader creates a reader for a run stored in the given temp file.
func NewRunReader(file *TempFile, run Run, codec *RowCodec, tinyWeighters []TinyWeighter, bufSize int) *RunReader {
	return &RunReader{
		codec:         codec,
		reader:        file.NewReader(run.Offset, run.Length, bufSize),
		remaining:     run.RowCount,
		tinyWeighters: tinyWeighters,
	}
}

// Next returns the next row from the run, or io.EOF when exhausted.
func (rr *RunReader) Next() (sqltypes.Row, error) {
	if rr.remaining <= 0 {
		return nil, io.EOF
	}

	// Read packet header
	if _, err := io.ReadFull(rr.reader, rr.hdr[:]); err != nil {
		return nil, err
	}
	payloadLen := int(rr.hdr[0]) | int(rr.hdr[1])<<8 | int(rr.hdr[2])<<16

	// Allocate payload from arena instead of per-row make()
	payload := rr.arena.alloc(payloadLen)
	if _, err := io.ReadFull(rr.reader, payload); err != nil {
		return nil, fmt.Errorf("sortio: reading row payload: %w", err)
	}

	row, err := rr.codec.ParseRow(payload)
	if err != nil {
		return nil, err
	}
	rr.remaining--

	// Reapply tinyweights since they are not serialized
	for _, tw := range rr.tinyWeighters {
		if tw.Col < len(row) && !row[tw.Col].IsNull() {
			tw.Apply(&row[tw.Col])
		}
	}

	return row, nil
}

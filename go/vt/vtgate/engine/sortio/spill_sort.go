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
	"bytes"
	"context"
	"io"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

const (
	// IntermediateMergeWay is the fan-in for intermediate merge passes that write to disk.
	IntermediateMergeWay = 7
	// FinalMergeWay is the fan-in for the final merge pass that streams directly to the callback.
	FinalMergeWay = 15
	// DefaultBufIOSize is the default buffer size for buffered I/O (64KB).
	DefaultBufIOSize = 64 * 1024
)

// SpillSorter implements an external merge sort. Rows are buffered in memory
// up to a configurable size limit, then spilled to disk as sorted runs.
// Runs are merged using a multi-pass merge with bounded fan-in.
type SpillSorter struct {
	cmp        evalengine.Comparison
	codec      *RowCodec
	bufferSize int64
	tmpDir     string
	fields     []*querypb.Field

	buffer      []sqltypes.Row
	bufferBytes int64
	runs        []Run
	files       [2]*TempFile
	currentFile int
	spilled     bool

	tinyWeighters []TinyWeighter
	encodeBuf     bytes.Buffer
}

// NewSpillSorter creates a new SpillSorter.
func NewSpillSorter(cmp evalengine.Comparison, fields []*querypb.Field, bufferSize int64, tmpDir string) *SpillSorter {
	ss := &SpillSorter{
		cmp:        cmp,
		codec:      NewRowCodec(fields),
		bufferSize: bufferSize,
		tmpDir:     tmpDir,
		fields:     fields,
	}
	ss.tinyWeighters = ss.buildTinyWeighters()
	return ss
}

func (ss *SpillSorter) buildTinyWeighters() []TinyWeighter {
	var weights []TinyWeighter
	for _, c := range ss.cmp {
		if c.Col >= len(ss.fields) {
			continue
		}
		if apply := evalengine.TinyWeighter(ss.fields[c.Col], c.Type.Collation()); apply != nil {
			weights = append(weights, TinyWeighter{Col: c.Col, Apply: apply})
		}
	}
	return weights
}

// Add adds a row to the sorter. If the in-memory buffer exceeds the configured
// size, the buffer is sorted and spilled to disk.
func (ss *SpillSorter) Add(row sqltypes.Row) error {
	rowSize := int64(ss.codec.EncodedSize(row))
	ss.buffer = append(ss.buffer, row)
	ss.bufferBytes += rowSize

	// Spill if we exceed the buffer, but always keep at least 1 row
	if ss.bufferBytes >= ss.bufferSize && len(ss.buffer) > 1 {
		return ss.spill()
	}
	return nil
}

func (ss *SpillSorter) ensureFile() error {
	if ss.files[ss.currentFile] == nil {
		f, err := NewTempFile(ss.tmpDir, DefaultBufIOSize)
		if err != nil {
			return err
		}
		ss.files[ss.currentFile] = f
	}
	return nil
}

func (ss *SpillSorter) spill() error {
	ss.spilled = true
	if err := ss.ensureFile(); err != nil {
		return err
	}

	// Apply tiny weights and sort the buffer
	ss.applyTinyWeights(ss.buffer)
	ss.cmp.Sort(ss.buffer)

	// Write sorted rows to the current temp file
	f := ss.files[ss.currentFile]
	startOffset := f.offset

	for _, row := range ss.buffer {
		ss.encodeBuf.Reset()
		ss.codec.Encode(&ss.encodeBuf, row)
		if _, err := f.Write(ss.encodeBuf.Bytes()); err != nil {
			return err
		}
	}

	ss.runs = append(ss.runs, Run{
		Offset:   startOffset,
		Length:   f.offset - startOffset,
		RowCount: len(ss.buffer),
	})

	ss.buffer = ss.buffer[:0]
	ss.bufferBytes = 0
	return nil
}

func (ss *SpillSorter) applyTinyWeights(rows []sqltypes.Row) {
	if len(ss.tinyWeighters) == 0 {
		return
	}
	for _, row := range rows {
		for _, tw := range ss.tinyWeighters {
			if tw.Col < len(row) && !row[tw.Col].IsNull() {
				tw.Apply(&row[tw.Col])
			}
		}
	}
}

// Finish completes the sort and streams sorted rows to the callback.
// If the callback returns io.EOF, iteration stops without error.
func (ss *SpillSorter) Finish(ctx context.Context, callback func(sqltypes.Row) error) error {
	if !ss.spilled {
		// Everything fits in memory
		ss.applyTinyWeights(ss.buffer)
		ss.cmp.Sort(ss.buffer)
		for _, row := range ss.buffer {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := callback(row); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
		ss.buffer = nil
		return nil
	}

	// Flush any remaining buffered rows as a final run
	if len(ss.buffer) > 0 {
		if err := ss.spill(); err != nil {
			return err
		}
	}

	// Flush the current temp file
	if f := ss.files[ss.currentFile]; f != nil {
		if err := f.Flush(); err != nil {
			return err
		}
	}

	return ss.mergeRuns(ctx, callback)
}

func (ss *SpillSorter) mergeRuns(ctx context.Context, callback func(sqltypes.Row) error) error {
	runs := ss.runs

	// Intermediate merge passes: reduce runs until <= FinalMergeWay
	for len(runs) > FinalMergeWay {
		if err := ctx.Err(); err != nil {
			return err
		}

		outFile := 1 - ss.currentFile
		if err := ss.ensureOtherFile(outFile); err != nil {
			return err
		}
		outTF := ss.files[outFile]

		var newRuns []Run
		for i := 0; i < len(runs); i += IntermediateMergeWay {
			end := min(i+IntermediateMergeWay, len(runs))
			chunk := runs[i:end]

			run, err := ss.mergeToFile(ctx, chunk, outTF)
			if err != nil {
				return err
			}
			newRuns = append(newRuns, run)
		}

		// Flush output file
		if err := outTF.Flush(); err != nil {
			return err
		}

		// Reset the input file for reuse
		if f := ss.files[ss.currentFile]; f != nil {
			if err := f.Reset(); err != nil {
				return err
			}
		}

		ss.currentFile = outFile
		runs = newRuns
	}

	// Final merge pass: stream directly to callback
	return ss.mergeToCallback(ctx, runs, callback)
}

func (ss *SpillSorter) ensureOtherFile(idx int) error {
	if ss.files[idx] == nil {
		f, err := NewTempFile(ss.tmpDir, DefaultBufIOSize)
		if err != nil {
			return err
		}
		ss.files[idx] = f
	}
	return nil
}

func (ss *SpillSorter) makeReaders(runs []Run) []*RunReader {
	readers := make([]*RunReader, len(runs))
	f := ss.files[ss.currentFile]
	for i, run := range runs {
		readers[i] = NewRunReader(f, run, ss.codec, ss.tinyWeighters, DefaultBufIOSize)
	}
	return readers
}

func (ss *SpillSorter) buildLoserTree(readers []*RunReader) (*loserTree, error) {
	entries := make([]mergeEntry, 0, len(readers))
	for i, rr := range readers {
		row, err := rr.Next()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, mergeEntry{row: row, source: i})
	}
	return newLoserTree(entries, ss.cmp.Less), nil
}

func (ss *SpillSorter) mergeToFile(ctx context.Context, runs []Run, outFile *TempFile) (Run, error) {
	readers := ss.makeReaders(runs)

	tree, err := ss.buildLoserTree(readers)
	if err != nil {
		return Run{}, err
	}

	startOffset := outFile.offset
	rowCount := 0

	for tree.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return Run{}, err
		}

		row, source := tree.Winner()

		ss.encodeBuf.Reset()
		ss.codec.Encode(&ss.encodeBuf, row)
		if _, err := outFile.Write(ss.encodeBuf.Bytes()); err != nil {
			return Run{}, err
		}
		rowCount++

		// Get next row from the same source
		nextRow, err := readers[source].Next()
		if err == io.EOF {
			tree.Remove()
			continue
		}
		if err != nil {
			return Run{}, err
		}
		tree.Replace(nextRow)
	}

	return Run{
		Offset:   startOffset,
		Length:   outFile.offset - startOffset,
		RowCount: rowCount,
	}, nil
}

func (ss *SpillSorter) mergeToCallback(ctx context.Context, runs []Run, callback func(sqltypes.Row) error) error {
	if len(runs) == 0 {
		return nil
	}

	// Special case: single run, just stream it
	if len(runs) == 1 {
		rr := NewRunReader(ss.files[ss.currentFile], runs[0], ss.codec, ss.tinyWeighters, DefaultBufIOSize)
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			row, err := rr.Next()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err := callback(row); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
	}

	readers := ss.makeReaders(runs)

	tree, err := ss.buildLoserTree(readers)
	if err != nil {
		return err
	}

	for tree.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		row, source := tree.Winner()
		if err := callback(row); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		nextRow, err := readers[source].Next()
		if err == io.EOF {
			tree.Remove()
			continue
		}
		if err != nil {
			return err
		}
		tree.Replace(nextRow)
	}

	return nil
}

// Close releases all temporary files.
func (ss *SpillSorter) Close() {
	for i, f := range ss.files {
		if f != nil {
			f.Close()
			ss.files[i] = nil
		}
	}
	ss.buffer = nil
	ss.runs = nil
}

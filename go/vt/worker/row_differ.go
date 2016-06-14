// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
)

// RowDiffer2 will consume rows on both sides, and compare them.
// It assumes left and right are sorted by ascending primary key.
// it will record errors if extra rows exist on either side.
type RowDiffer2 struct {
	left         *RowReader
	right        *RowReader
	pkFieldCount int
}

// NewRowDiffer2 returns a new RowDiffer2
func NewRowDiffer2(left, right *QueryResultReader, tableDefinition *tabletmanagerdatapb.TableDefinition) (*RowDiffer2, error) {
	if len(left.Fields) != len(right.Fields) {
		return nil, fmt.Errorf("Cannot diff inputs with different types")
	}
	for i, field := range left.Fields {
		if field.Type != right.Fields[i].Type {
			return nil, fmt.Errorf("Cannot diff inputs with different types: field %v types are %v and %v", i, field.Type, right.Fields[i].Type)
		}
	}
	return &RowDiffer2{
		left:         NewRowReader(left),
		right:        NewRowReader(right),
		pkFieldCount: len(tableDefinition.PrimaryKeyColumns),
	}, nil
}

// Go runs the diff. If there is no error, it will drain both sides.
// If an error occurs, it will just return it and stop.
func (rd *RowDiffer2) Go(log logutil.Logger) (DiffReport, error) {

	var dr DiffReport
	var err error

	dr.startingTime = time.Now()
	defer dr.ComputeQPS()

	var left []sqltypes.Value
	var right []sqltypes.Value
	advanceLeft := true
	advanceRight := true
	for {
		if advanceLeft {
			if left, err = rd.left.Next(); err != nil {
				return dr, err
			}
			advanceLeft = false
		}
		if advanceRight {
			if right, err = rd.right.Next(); err != nil {
				return dr, err
			}
			advanceRight = false
		}
		dr.processedRows++
		if left == nil {
			// no more rows from the left
			if right == nil {
				// no more rows from right either, we're done
				return dr, nil
			}

			// drain right, update count
			count, err := rd.right.Drain()
			if err != nil {
				return dr, err
			}
			dr.extraRowsRight += 1 + count
			return dr, nil
		}
		if right == nil {
			// no more rows from the right
			// we know we have rows from left, drain, update count
			count, err := rd.left.Drain()
			if err != nil {
				return dr, err
			}
			dr.extraRowsLeft += 1 + count
			return dr, nil
		}

		// we have both left and right, compare
		f := RowsEqual(left, right)
		if f == -1 {
			// rows are the same, next
			dr.matchingRows++
			advanceLeft = true
			advanceRight = true
			continue
		}

		if f >= rd.pkFieldCount {
			// rows have the same primary key, only content is different
			if dr.mismatchedRows < 10 {
				log.Errorf("Different content %v in same PK: %v != %v", dr.mismatchedRows, left, right)
			}
			dr.mismatchedRows++
			advanceLeft = true
			advanceRight = true
			continue
		}

		// have to find the 'smallest' row and advance it
		c, err := CompareRows(rd.left.Fields(), rd.pkFieldCount, left, right)
		if err != nil {
			return dr, err
		}
		if c < 0 {
			if dr.extraRowsLeft < 10 {
				log.Errorf("Extra row %v on left: %v", dr.extraRowsLeft, left)
			}
			dr.extraRowsLeft++
			advanceLeft = true
			continue
		} else if c > 0 {
			if dr.extraRowsRight < 10 {
				log.Errorf("Extra row %v on right: %v", dr.extraRowsRight, right)
			}
			dr.extraRowsRight++
			advanceRight = true
			continue
		}

		// After looking at primary keys more carefully,
		// they're the same. Logging a regular difference
		// then, and advancing both.
		if dr.mismatchedRows < 10 {
			log.Errorf("Different content %v in same PK: %v != %v", dr.mismatchedRows, left, right)
		}
		dr.mismatchedRows++
		advanceLeft = true
		advanceRight = true
	}
}

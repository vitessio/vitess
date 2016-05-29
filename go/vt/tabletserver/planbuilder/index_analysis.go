// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

type indexScore struct {
	Index       *schema.Index
	ColumnMatch []bool
	MatchFailed bool
}

type scoreValue int64

const (
	noMatch      = scoreValue(-1)
	perfectScore = scoreValue(0)
)

func newIndexScore(index *schema.Index) *indexScore {
	return &indexScore{index, make([]bool, len(index.Columns)), false}
}

func (is *indexScore) FindMatch(columnName string) int {
	if is.MatchFailed {
		return -1
	}
	if index := is.Index.FindColumn(columnName); index != -1 {
		is.ColumnMatch[index] = true
		return index
	}
	// If the column is among the data columns, we can still use
	// the index without going to the main table
	if index := is.Index.FindDataColumn(columnName); index == -1 {
		is.MatchFailed = true
	}
	return -1
}

func (is *indexScore) GetScore() scoreValue {
	if is.MatchFailed {
		return noMatch
	}
	score := noMatch
	for i, indexColumn := range is.ColumnMatch {
		if indexColumn {
			score = scoreValue(is.Index.Cardinality[i])
			continue
		}
		return score
	}
	return perfectScore
}

func getPKValues(conditions []sqlparser.BoolExpr, pkIndex *schema.Index) (pkValues []interface{}, err error) {
	pkindexScore := newIndexScore(pkIndex)
	pkValues = make([]interface{}, len(pkindexScore.ColumnMatch))
	for _, condition := range conditions {
		condition, ok := condition.(*sqlparser.ComparisonExpr)
		if !ok {
			return nil, nil
		}
		if !sqlparser.StringIn(condition.Operator, sqlparser.EqualStr, sqlparser.InStr) {
			return nil, nil
		}
		index := pkindexScore.FindMatch(condition.Left.(*sqlparser.ColName).Name.Original())
		if index == -1 {
			return nil, nil
		}
		switch condition.Operator {
		case sqlparser.EqualStr, sqlparser.InStr:
			var err error
			pkValues[index], err = sqlparser.AsInterface(condition.Right)
			if err != nil {
				return nil, err
			}
		default:
			panic("unreachable")
		}
	}
	if pkindexScore.GetScore() == perfectScore {
		return pkValues, nil
	}
	return nil, nil
}

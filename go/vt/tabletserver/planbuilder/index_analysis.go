// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

type IndexScore struct {
	Index       *schema.Index
	ColumnMatch []bool
	MatchFailed bool
}

type scoreValue int64

const (
	NO_MATCH      = scoreValue(-1)
	PERFECT_SCORE = scoreValue(0)
)

func NewIndexScore(index *schema.Index) *IndexScore {
	return &IndexScore{index, make([]bool, len(index.Columns)), false}
}

func (is *IndexScore) FindMatch(columnName string) int {
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

func (is *IndexScore) GetScore() scoreValue {
	if is.MatchFailed {
		return NO_MATCH
	}
	score := NO_MATCH
	for i, indexColumn := range is.ColumnMatch {
		if indexColumn {
			score = scoreValue(is.Index.Cardinality[i])
			continue
		}
		return score
	}
	return PERFECT_SCORE
}

func NewIndexScoreList(indexes []*schema.Index) []*IndexScore {
	scoreList := make([]*IndexScore, len(indexes))
	for i, v := range indexes {
		scoreList[i] = NewIndexScore(v)
	}
	return scoreList
}

func getSelectPKValues(conditions []sqlparser.BoolExpr, pkIndex *schema.Index) (planId PlanType, pkValues []interface{}, err error) {
	pkValues, err = getPKValues(conditions, pkIndex)
	if err != nil {
		return 0, nil, err
	}
	if pkValues == nil {
		return PLAN_PASS_SELECT, nil, nil
	}
	for _, pkValue := range pkValues {
		inList, ok := pkValue.([]interface{})
		if !ok {
			continue
		}
		if len(pkValues) == 1 {
			return PLAN_PK_IN, inList, nil
		}
		return PLAN_PASS_SELECT, nil, nil
	}
	return PLAN_PK_EQUAL, pkValues, nil
}

func getPKValues(conditions []sqlparser.BoolExpr, pkIndex *schema.Index) (pkValues []interface{}, err error) {
	pkIndexScore := NewIndexScore(pkIndex)
	pkValues = make([]interface{}, len(pkIndexScore.ColumnMatch))
	for _, condition := range conditions {
		condition, ok := condition.(*sqlparser.ComparisonExpr)
		if !ok {
			return nil, nil
		}
		if !sqlparser.StringIn(condition.Operator, sqlparser.AST_EQ, sqlparser.AST_IN) {
			return nil, nil
		}
		index := pkIndexScore.FindMatch(string(condition.Left.(*sqlparser.ColName).Name))
		if index == -1 {
			return nil, nil
		}
		switch condition.Operator {
		case sqlparser.AST_EQ, sqlparser.AST_IN:
			var err error
			pkValues[index], err = sqlparser.AsInterface(condition.Right)
			if err != nil {
				return nil, err
			}
		default:
			panic("unreachable")
		}
	}
	if pkIndexScore.GetScore() == PERFECT_SCORE {
		return pkValues, nil
	}
	return nil, nil
}

func getIndexMatch(conditions []sqlparser.BoolExpr, indexes []*schema.Index) string {
	indexScores := NewIndexScoreList(indexes)
	for _, condition := range conditions {
		var col string
		switch condition := condition.(type) {
		case *sqlparser.ComparisonExpr:
			col = string(condition.Left.(*sqlparser.ColName).Name)
		case *sqlparser.RangeCond:
			col = string(condition.Left.(*sqlparser.ColName).Name)
		default:
			panic("unreachaable")
		}
		for _, index := range indexScores {
			index.FindMatch(col)
		}
	}
	highScore := NO_MATCH
	highScorer := -1
	for i, index := range indexScores {
		curScore := index.GetScore()
		if curScore == NO_MATCH {
			continue
		}
		if curScore == PERFECT_SCORE {
			highScorer = i
			break
		}
		// Prefer secondary index over primary key
		if curScore >= highScore {
			highScore = curScore
			highScorer = i
		}
	}
	if highScorer == -1 {
		return ""
	}
	return indexes[highScorer].Name
}

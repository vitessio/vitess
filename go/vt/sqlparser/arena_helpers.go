/*
Copyright 2024 The Vitess Authors.

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

package sqlparser

import "strings"

// Arena-aware helper functions for AST node construction. These are called
// from grammar rule actions in sql.y where `yyrcvr.Arena` is passed as the
// first argument. When arena is nil, they fall back to heap allocation.

func newStrLiteralA(a *Arena, in string) *Literal {
	return a.newLiteralV(Literal{Type: StrVal, Val: in})
}

func newIntLiteralA(a *Arena, in string) *Literal {
	return a.newLiteralV(Literal{Type: IntVal, Val: in})
}

func newDecimalLiteralA(a *Arena, in string) *Literal {
	return a.newLiteralV(Literal{Type: DecimalVal, Val: in})
}

func newFloatLiteralA(a *Arena, in string) *Literal {
	return a.newLiteralV(Literal{Type: FloatVal, Val: in})
}

func newHexNumLiteralA(a *Arena, in string) *Literal {
	return a.newLiteralV(Literal{Type: HexNum, Val: in})
}

func newHexLiteralA(a *Arena, in string) *Literal {
	return a.newLiteralV(Literal{Type: HexVal, Val: in})
}

func newBitLiteralA(a *Arena, in string) *Literal {
	return a.newLiteralV(Literal{Type: BitVal, Val: in})
}

func newDateLiteralA(a *Arena, in string) *Literal {
	return a.newLiteralV(Literal{Type: DateVal, Val: in})
}

func newTimeLiteralA(a *Arena, in string) *Literal {
	return a.newLiteralV(Literal{Type: TimeVal, Val: in})
}

func newTimestampLiteralA(a *Arena, in string) *Literal {
	return a.newLiteralV(Literal{Type: TimestampVal, Val: in})
}

func newWhereA(a *Arena, typ WhereType, expr Expr) *Where {
	if expr == nil {
		return nil
	}
	return a.newWhereV(Where{Type: typ, Expr: expr})
}

func newSelectA(a *Arena, comments Comments, exprs SelectExprs, selectOptions []string, into *SelectInto, from TableExprs, where *Where, groupBy GroupBy, having *Where, windows NamedWindows) *Select {
	var cache *bool
	var distinct, straightJoinHint, sqlFoundRows bool

	for _, option := range selectOptions {
		switch strings.ToLower(option) {
		case DistinctStr:
			distinct = true
		case SQLCacheStr:
			truth := true
			cache = &truth
		case SQLNoCacheStr:
			truth := false
			cache = &truth
		case StraightJoinHint:
			straightJoinHint = true
		case SQLCalcFoundRowsStr:
			sqlFoundRows = true
		}
	}
	return a.newSelectV(Select{
		Cache:            cache,
		Comments:         comments.Parsed(),
		Distinct:         distinct,
		StraightJoinHint: straightJoinHint,
		SQLCalcFoundRows: sqlFoundRows,
		SelectExprs:      exprs,
		Into:             into,
		From:             from,
		Where:            where,
		GroupBy:          groupBy,
		Having:           having,
		Windows:          windows,
	})
}

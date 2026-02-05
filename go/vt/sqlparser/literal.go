/*
Copyright 2023 The Vitess Authors.

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

import (
	"errors"
	"fmt"
	"math"
	"math/big"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/fastparse"
	"vitess.io/vitess/go/mysql/hex"
	"vitess.io/vitess/go/sqltypes"
)

func LiteralToValue(lit *Literal) (sqltypes.Value, error) {
	switch lit.Type {
	case IntVal:
		uval, err := fastparse.ParseUint64(lit.Val, 10)
		if err != nil {
			if errors.Is(err, fastparse.ErrOverflow) {
				return sqltypes.NewDecimal(lit.Val), nil
			}
			return sqltypes.Value{}, err
		}
		if uval <= math.MaxInt64 {
			return sqltypes.NewInt64(int64(uval)), nil
		}
		return sqltypes.NewUint64(uval), nil
	case FloatVal:
		fval, err := fastparse.ParseFloat64(lit.Val)
		if err != nil {
			return sqltypes.Value{}, err
		}
		return sqltypes.NewFloat64(fval), nil
	case DecimalVal:
		dec, err := decimal.NewFromMySQL(lit.Bytes())
		if err != nil {
			return sqltypes.Value{}, err
		}
		return sqltypes.NewDecimal(hack.String(dec.FormatMySQL(0))), nil
	case StrVal:
		return sqltypes.NewVarChar(lit.Val), nil
	case HexNum:
		b := lit.Bytes()
		if b[0] != '0' || b[1] != 'x' {
			return sqltypes.Value{}, fmt.Errorf("invalid hex literal: %v", lit.Val)
		}
		if len(lit.Val)%2 == 0 {
			return parseHexLiteral(b[2:])
		}
		// If the hex literal doesn't have an even amount of hex digits, we need
		// to pad it with a '0' in the left. Instead of allocating a new slice
		// for padding pad in-place by replacing the 'x' in the original slice with
		// a '0', and clean it up after parsing.
		b[1] = '0'
		defer func() {
			b[1] = 'x'
		}()
		return parseHexLiteral(b[1:])
	case HexVal:
		return parseHexLiteral(lit.Bytes())
	case BitNum:
		return parseBitLiteral(lit.Bytes())
	case DateVal:
		d, ok := datetime.ParseDate(lit.Val)
		if !ok {
			return sqltypes.Value{}, fmt.Errorf("invalid time literal: %v", lit.Val)
		}
		buf := datetime.Date_YYYY_MM_DD.Format(datetime.DateTime{Date: d}, 0)
		return sqltypes.NewDate(hack.String(buf)), nil
	case TimeVal:
		t, l, state := datetime.ParseTime(lit.Val, -1)
		if state != datetime.TimeOK {
			return sqltypes.Value{}, fmt.Errorf("invalid time literal: %v", lit.Val)
		}
		buf := datetime.Time_hh_mm_ss.Format(datetime.DateTime{Time: t}, uint8(l))
		return sqltypes.NewTime(hack.String(buf)), nil
	case TimestampVal:
		dt, l, ok := datetime.ParseDateTime(lit.Val, -1)
		if !ok {
			return sqltypes.Value{}, fmt.Errorf("invalid time literal: %v", lit.Val)
		}
		buf := datetime.DateTime_YYYY_MM_DD_hh_mm_ss.Format(dt, uint8(l))
		return sqltypes.NewDatetime(hack.String(buf)), nil
	default:
		return sqltypes.Value{}, fmt.Errorf("unsupported literal type: %v", lit.Type)
	}
}

func parseHexLiteral(val []byte) (sqltypes.Value, error) {
	raw := make([]byte, hex.DecodedLen(val))
	if err := hex.DecodeBytes(raw, val); err != nil {
		return sqltypes.Value{}, err
	}
	return sqltypes.NewVarBinary(hack.String(raw)), nil
}

func parseBitLiteral(val []byte) (sqltypes.Value, error) {
	var i big.Int
	_, ok := i.SetString(string(val), 2)
	if !ok {
		return sqltypes.Value{}, fmt.Errorf("invalid bit literal: %v", val)
	}
	return sqltypes.NewVarBinary(hack.String(i.Bytes())), nil
}

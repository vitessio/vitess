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

package mysql

import (
	"bytes"
	"strconv"

	"vitess.io/vitess/go/sqltypes"
)

// FormatFloat formats a float64 as a byte string in a similar way to what MySQL does
func FormatFloat(typ sqltypes.Type, f float64) []byte {
	return AppendFloat(nil, typ, f)
}

func AppendFloat(buf []byte, typ sqltypes.Type, f float64) []byte {
	format := byte('g')
	if typ == sqltypes.Decimal {
		format = 'f'
	}

	// the float printer in MySQL does not add a positive sign before
	// the exponent for positive exponents, but the Golang printer does
	// do that, and there's no way to customize it, so we must strip the
	// redundant positive sign manually
	// e.g. 1.234E+56789 -> 1.234E56789
	fstr := strconv.AppendFloat(buf, f, format, -1, 64)
	if idx := bytes.IndexByte(fstr, 'e'); idx >= 0 {
		if fstr[idx+1] == '+' {
			fstr = append(fstr[:idx+1], fstr[idx+2:]...)
		}
	}

	return fstr
}

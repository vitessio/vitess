// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"strconv"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// Result represents a query result.
type Result struct {
	Fields       []*querypb.Field
	RowsAffected uint64
	InsertID     uint64
	Rows         [][]Value
}

// Convert takes a field and a value, and returns the native type:
// - nil for NULL value
// - uint64 for unsigned values
// - int64 for signed values
// - float64 for floating point values
// - []byte for everything else
// TODO(sougou): move this to Value once it's full-featured.
func Convert(field *querypb.Field, val Value) (interface{}, error) {
	switch {
	case field.Type == Null:
		return nil, nil
	case IsSigned(field.Type):
		return strconv.ParseInt(val.String(), 0, 64)
	case IsUnsigned(field.Type):
		return strconv.ParseUint(val.String(), 0, 64)
	case IsFloat(field.Type):
		return strconv.ParseFloat(val.String(), 64)
	}
	return val.Raw(), nil
}

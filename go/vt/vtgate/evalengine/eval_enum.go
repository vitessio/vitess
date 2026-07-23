/*
Copyright 2026 The Vitess Authors.

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

package evalengine

import (
	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vthash"
)

type evalEnum struct {
	value  int
	string string
}

func newEvalEnum(val []byte, values *EnumSetValues) *evalEnum {
	s := string(val)
	return &evalEnum{
		value:  valueIdx(values, s),
		string: s,
	}
}

func (e *evalEnum) ToRawBytes() []byte {
	return hack.StringBytes(e.string)
}

func (e *evalEnum) SQLType() sqltypes.Type {
	return sqltypes.Enum
}

func (e *evalEnum) Size() int32 {
	return 0
}

func (e *evalEnum) Scale() int32 {
	return 0
}

// Hash implements the hashable interface for evalEnum.
// For enums that match their declared values list, we hash their ordinal value;
// for any values that could not be resolved to an ordinal (value==-1), we fall back to
// hashing their raw string bytes using the binary collation.
func (e *evalEnum) Hash(h *vthash.Hasher) {
	if e.value == -1 {
		// For unknown values, fall back to hashing the string contents.
		h.Write16(hashPrefixBytes)
		colldata.Lookup(collations.CollationBinaryID).Hash(h, hack.StringBytes(e.string), 0)
		return
	}
	// Enums are positive integral values starting at zero internally.
	h.Write16(hashPrefixIntegralPositive)
	h.Write64(uint64(e.value))
}

// enumNumeric returns the value of an enum in a numeric context. MySQL uses the
// 1-based ordinal of the value within the enum definition. A negative value is
// the sentinel for a value that could not be resolved against the enum
// definition (e.g. the definition was not available); it is passed through
// unchanged to preserve the existing best-effort behavior for that case.
func enumNumeric(value int) int64 {
	if value < 0 {
		return int64(value)
	}
	return int64(value) + 1
}

func valueIdx(values *EnumSetValues, value string) int {
	if values == nil {
		return -1
	}
	for i, v := range *values {
		v, _ = sqltypes.DecodeStringSQL(v)
		if v == value {
			return i
		}
	}
	return -1
}

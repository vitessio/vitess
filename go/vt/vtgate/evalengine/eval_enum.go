package evalengine

import (
	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/sqltypes"
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

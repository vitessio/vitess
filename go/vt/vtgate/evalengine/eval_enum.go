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

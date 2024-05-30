package evalengine

import (
	"strings"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/sqltypes"
)

type evalSet struct {
	set    uint64
	string string
}

func newEvalSet(val []byte, values *EnumSetValues) *evalSet {
	value := string(val)

	return &evalSet{
		set:    evalSetBits(values, value),
		string: value,
	}
}

func (e *evalSet) ToRawBytes() []byte {
	return hack.StringBytes(e.string)
}

func (e *evalSet) SQLType() sqltypes.Type {
	return sqltypes.Set
}

func (e *evalSet) Size() int32 {
	return 0
}

func (e *evalSet) Scale() int32 {
	return 0
}

func evalSetBits(values *EnumSetValues, value string) uint64 {
	if values != nil && len(*values) > 64 {
		// This never would happen as MySQL limits SET
		// to 64 elements. Safeguard here just in case though.
		panic("too many values for set")
	}

	set := uint64(0)
	for _, val := range strings.Split(value, ",") {
		idx := valueIdx(values, val)
		if idx == -1 {
			continue
		}
		set |= 1 << idx
	}

	return set
}

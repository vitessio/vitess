package evalengine

import (
	"strings"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vthash"
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

// Hash implements the hashable interface for evalSet.
// For sets where all elements resolve to ordinals in the declared set, we hash the bitset; otherwise,
// if the set string contained values that cannot be mapped to ordinals, we fall back to hashing
// the raw string using the binary collation. This ensures DISTINCT and other hash-based operations
// treat unknown sets as distinct based on their textual representation.
func (e *evalSet) Hash(h *vthash.Hasher) {
	// MySQL allows storing an empty set as an empty string, which yields set==0 and string=="";
	// unknown sets will have set==0 but non-empty string content.
	if e.set == 0 && e.string != "" {
		h.Write16(hashPrefixBytes)
		colldata.Lookup(collations.CollationBinaryID).Hash(h, hack.StringBytes(e.string), 0)
		return
	}
	h.Write16(hashPrefixIntegralPositive)
	h.Write64(e.set)
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

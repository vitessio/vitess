/*
Copyright 2021 The Vitess Authors.

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

package vindexes

import (
	"bytes"
	"context"
	"encoding/json"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	cfcParamHash    = "hash"
	cfcParamOffsets = "offsets"
)

var (
	_ ParamValidating = (*CFC)(nil)

	cfcParams = []string{
		cfcParamHash,
		cfcParamOffsets,
	}
)

// CFC is Concatenated Fixed-width Composite Vindex.
//
// The purpose of this vindex is to shard the rows based on the prefix of
// sharding key. Imagine the sharding key is defined as (s1, s2, ... sN), a
// prefix of this key is (s1, s2, ... sj) (j <= N). This vindex puts the rows
// with the same prefix among a same group of shards instead of scatter them
// around all the shards. The benefit of doing so is that prefix queries will
// only fanout to a subset of shards instead of all the shards. Specifically
// this vindex maps the full key, i.e. (s1, s2, ... sN) to a
// `key.DestinationKeyspaceID` and the prefix of it, i.e. (s1, s2, ... sj)(j<N)
// to a `key.DestinationKeyRange`. Note that the prefix to key range mapping is
// only active in 'LIKE' expression. When a column with CFC defined appears in
// other expressions, e.g. =, !=, IN etc, it behaves exactly as other
// functional unique vindexes.
//
// This provides the capability to model hierarchical data models. If we
// consider the prefix as the 'parent' key and the full key as the 'child' key,
// all the child data is clustered within the same group of shards identified
// by the 'parent' key.
//
// Due to the prevalance of using `vindexes.SingleColumn` in vindexes, it's way
// more complex to implement a true multi-column composite index (see github
// issue) than to implement it using a single column vindex where the
// components of the composite keys are concatenated together to form a single
// key. The user can use this single key directly as the keyspace id; one can
// also define a hash function so that the keyspace id is the concatenation of
// hash(s1), hash(s2), ... hash(sN). Using the concatenated key directly makes
// it easier to reason the fanout but the data distribution depends on the key
// itself; while using the hash on components takes care of the randomness of
// the data distribution.
//
// Since the vindex is on a concatenated key, the offsets into the key are the
// only way to mark its components. Thus it implicitly requires each component
// to have a fixed width, except the last one. It's especially true when hash
// is defined. Because the hash is calculated component by component, only the
// prefix that aligns with the component boundary can be used to compute the
// key range. Although the misaligned part doesn't participate the key range
// calculation, the SQL executed on each shard uses the unchanged prefix; thus
// the behavior is exactly same as other vindex's but just more efficient in
// controlling the fanout.
//
// # The expected format of the vindex definition is
//
//	"vindexes": {
//	  "cfc_md5": {
//	    "type": "cfc",
//	    "params": {
//	      "hash": "md5",
//	      "offsets": "[2,4]"
//	    }
//	  }
//	}
//
// 'offsets' only makes sense when hash is used. Offsets should be a sorted
// list of positive ints, each of which denotes the byte offset (from the
// beginning of key) of each component's boundary in the concatenated key.
// Specifically, offsets[0] is the byte offset of the first component,
// offsets[1] is the byte offset of the second component, etc.
type CFC struct {
	// CFC is used in all compare expressions other than 'LIKE'.
	*cfcCommon
	// prefixCFC is only used in 'LIKE' compare expressions.
	prefixCFC *prefixCFC
}

type cfcCommon struct {
	name          string
	hash          func([]byte) []byte
	offsets       []int
	unknownParams []string
}

// newCFC creates a new CFC vindex
func newCFC(name string, params map[string]string) (Vindex, error) {
	ss := &cfcCommon{
		name:          name,
		unknownParams: FindUnknownParams(params, cfcParams),
	}
	cfc := &CFC{
		cfcCommon: ss,
		prefixCFC: &prefixCFC{cfcCommon: ss},
	}

	if params == nil {
		return cfc, nil
	}

	switch h := params[cfcParamHash]; h {
	case "":
		return cfc, nil
	case "md5":
		ss.hash = md5hash
	case "xxhash64":
		ss.hash = xxhash64
	default:
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid hash %s to CFC vindex %s", h, name)
	}

	var offsets []int
	if p := params[cfcParamOffsets]; p == "" {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "CFC vindex requires offsets when hash is defined")
	} else if err := json.Unmarshal([]byte(p), &offsets); err != nil || !validOffsets(offsets) {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid offsets %s to CFC vindex %s. expected sorted positive ints in brackets", p, name)
	}
	// remove duplicates
	prev := -1
	for _, off := range offsets {
		if off != prev {
			ss.offsets = append(ss.offsets, off)
		}
		prev = off
	}

	return cfc, nil
}

func validOffsets(offsets []int) bool {
	n := len(offsets)
	if n == 0 {
		return false
	}
	if offsets[0] <= 0 {
		return false
	}

	for i := 1; i < n; i++ {
		if offsets[i] <= offsets[i-1] {
			return false
		}
	}
	return true
}

func (vind *CFC) String() string {
	return vind.name
}

// Cost returns the cost as 1. In regular mode, i.e. not in a LIKE op, CFC has
// pretty much the same cost as other unique vindexes like 'binary', 'md5' etc.
func (vind *CFC) Cost() int {
	return 1
}

// IsUnique returns true since the Vindex is unique.
func (vind *CFC) IsUnique() bool {
	return true
}

// NeedsVCursor satisfies the Vindex interface.
func (vind *CFC) NeedsVCursor() bool {
	return false
}

// computeKsid returns the corresponding keyspace id of a key.
func (vind *cfcCommon) computeKsid(v []byte, prefix bool) ([]byte, error) {

	if vind.hash == nil {
		return v, nil
	}
	n := len(v)
	m := len(vind.offsets)
	// if we are not working on a prefix, the key has to have all the components,
	// that is, it has to be longer than the largest offset.
	if !prefix && n < vind.offsets[m-1] {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "insufficient size for cfc vindex %s. need %d, got %d", vind.name, vind.offsets[m-1], n)
	}
	prev := 0
	offset := 0
	buf := new(bytes.Buffer)
	for _, offset = range vind.offsets {
		if n < offset {
			// the given prefix doesn't align with the component boundaries,
			// return the hashed prefix that's currently available
			return buf.Bytes(), nil
		}

		if _, err := buf.Write(vind.hash(v[prev:offset])); err != nil {
			return nil, err
		}
		prev = offset
	}
	if offset < n {
		if _, err := buf.Write(vind.hash(v[offset:n])); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (vind *cfcCommon) verify(ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		idBytes, err := ids[i].ToBytes()
		if err != nil {
			return out, err
		}
		v, err := vind.computeKsid(idBytes, false)
		if err != nil {
			return nil, err
		}
		out[i] = bytes.Equal(v, ksids[i])
	}
	return out, nil
}

// UnknownParams implements the ParamValidating interface.
func (vind *cfcCommon) UnknownParams() []string {
	return vind.unknownParams
}

// Verify returns true if ids maps to ksids.
func (vind *CFC) Verify(_ context.Context, _ VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	return vind.verify(ids, ksids)
}

// Map can map ids to key.Destination objects.
func (vind *CFC) Map(_ context.Context, _ VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, len(ids))
	for i, id := range ids {
		idBytes, err := id.ToBytes()
		if err != nil {
			return out, err
		}
		v, err := vind.computeKsid(idBytes, false)
		if err != nil {
			return nil, err
		}
		out[i] = key.DestinationKeyspaceID(v)
	}
	return out, nil
}

// PrefixVindex switches the vindex to prefix mode
func (vind *CFC) PrefixVindex() SingleColumn {
	return vind.prefixCFC
}

// NewKeyRangeFromPrefix creates a keyspace range from a prefix of keyspace id.
func NewKeyRangeFromPrefix(begin []byte) key.Destination {
	if len(begin) == 0 {
		return key.DestinationAllShards{}
	}
	// the prefix maps to a keyspace range corresponding to its value and plus one.
	// that is [ keyspace_id, keyspace_id + 1 ).
	end := make([]byte, len(begin))
	copy(end, begin)
	end = addOne(end)
	return key.DestinationKeyRange{
		KeyRange: &topodatapb.KeyRange{
			Start: begin,
			End:   end,
		},
	}
}

func addOne(value []byte) []byte {
	n := len(value)
	overflow := true
	for i := n - 1; i >= 0; i-- {
		if value[i] < 255 {
			value[i]++
			overflow = false
			break
		} else {
			value[i] = 0
		}
	}
	if overflow {
		return nil
	}
	return value
}

type prefixCFC struct {
	*cfcCommon
}

func (vind *prefixCFC) String() string {
	return vind.name
}

func (vind *prefixCFC) NeedsVCursor() bool {
	return false
}

func (vind *prefixCFC) Verify(_ context.Context, _ VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	return vind.verify(ids, ksids)
}

// In prefix mode, i.e. within a LIKE op, the cost is higher than regular mode.
// Ideally the cost should be the number of shards we resolved to but the current
// framework doesn't do dynamic cost evaluation.
func (vind *prefixCFC) Cost() int {
	if n := len(vind.offsets); n > 0 {
		return n
	}
	return 2
}

func (vind *prefixCFC) IsUnique() bool {
	return false
}

// Map can map ids to key.Destination objects.
func (vind *prefixCFC) Map(_ context.Context, _ VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, len(ids))
	for i, id := range ids {
		value, err := id.ToBytes()
		if err != nil {
			return out, err
		}
		prefix := findPrefix(value)
		begin, err := vind.computeKsid(prefix, true)
		if err != nil {
			return nil, err
		}
		out[i] = NewKeyRangeFromPrefix(begin)
	}
	return out, nil
}

// findPrefix returns the 'prefix' of the string literal in LIKE expression.
// The prefix is the prefix of the string literal up until the first unescaped
// meta character (% and _). Other escape sequences are escaped according to
// https://dev.mysql.com/doc/refman/8.0/en/string-literals.html.
func findPrefix(str []byte) []byte {
	buf := new(bytes.Buffer)
L:
	for len(str) > 0 {
		n := len(str)
		p := bytes.IndexAny(str, `%_\`)
		if p < 0 {
			buf.Write(str)
			break
		}
		buf.Write(str[:p])
		switch str[p] {
		case '%', '_':
			// prefix found
			break L
		// The following is not very efficient in dealing with too many
		// continuous backslash characters, e.g. '\\\\\\\\\\\\\%', but
		// hopefully it's the less common case.
		case '\\':
			if p == n-1 {
				// backslash is the very last character of a string, typically
				// this is an invalid string argument. We write the backslash
				// anyway because Mysql can deal with it.
				buf.WriteByte(str[p])
				break L
			} else if decoded := sqltypes.SQLDecodeMap[str[p+1]]; decoded != sqltypes.DontEscape {
				buf.WriteByte(decoded)
			} else {
				buf.WriteByte(str[p+1])
			}
			str = str[(p + 2):n]
		}
	}
	return buf.Bytes()
}

// we don't use the full hashed value because it's very long.
// keyrange resolution is done via comparing []byte so longer
// keyspace ids have performance impact.
func md5hash(in []byte) []byte {
	n := len(in)
	out := vMD5Hash(in)
	if n < len(out) {
		return out[:n]
	}
	return out

}

// same here
func xxhash64(in []byte) []byte {
	out := vXXHash(in)
	n := len(in)
	if n < len(out) {
		return out[:n]
	}
	return out
}

func init() {
	Register("cfc", newCFC)
}

/*
Copyright 2017 Google Inc.

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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_        Vindex = (*Null)(nil)
	nullksid        = []byte{0}
)

// Null defines a vindex that always return 0. It's Unique and
// Functional.
// This is useful for rows that always go into the first shard.
// This Vindex can be used for validating an unsharded->sharded transition.
// Unlike other vindexes, this one will work even for NULL input values. This
// will allow you to keep MySQL auto-inc columns unchanged.
type Null struct {
	name string
}

// NewNull creates a new Null.
func NewNull(name string, m map[string]string) (Vindex, error) {
	return &Null{name: name}, nil
}

// String returns the name of the vindex.
func (vind *Null) String() string {
	return vind.name
}

// Cost returns the cost of this index as 0.
func (vind *Null) Cost() int {
	return 0
}

// IsUnique returns true since the Vindex is unique.
func (vind *Null) IsUnique() bool {
	return true
}

// IsFunctional returns true since the Vindex is functional.
func (vind *Null) IsFunctional() bool {
	return true
}

// Map can map ids to key.Destination objects.
func (vind *Null) Map(cursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	for i := 0; i < len(ids); i++ {
		out = append(out, key.DestinationKeyspaceID(nullksid))
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (vind *Null) Verify(cursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		out[i] = bytes.Equal(nullksid, ksids[i])
	}
	return out, nil
}

func init() {
	Register("null", NewNull)
}

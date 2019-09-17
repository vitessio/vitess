/*
Copyright 2019 The Vitess Authors.

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

package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
)

// All these tests are constructing a HashJoin, but testing the implementation in the single_int_hash_join file

func TestSingleHit(t *testing.T) {
	p := newSingleIntProbeTable(1)
	r1 := values(1, 2, 3)
	r2 := values(1, 4, 3)
	r3 := values(1, 5, 3)

	assert.NoError(t,
		p.Add(r1))
	assert.NoError(t,
		p.Add(r2))
	assert.NoError(t,
		p.Add(r3))

	get, err := p.Get(cast(4))
	assert.NoError(t, err)

	assert.Equal(t, [][]sqltypes.Value{r2}, get)
}

func TestNoHits(t *testing.T) {
	p := newSingleIntProbeTable(1)
	r1 := values(1, 2, 3)
	r2 := values(1, 4, 3)
	r3 := values(1, 5, 3)

	assert.NoError(t,
		p.Add(r1))
	assert.NoError(t,
		p.Add(r2))
	assert.NoError(t,
		p.Add(r3))

	get, err := p.Get(cast(66))
	assert.NoError(t, err)

	assert.Equal(t, [][]sqltypes.Value{}, get)

}

func TestMultiplesHit(t *testing.T) {
	p := newSingleIntProbeTable(1)
	r1 := values(1, 2, 6)
	r2 := values(1, 4, 3)
	r3 := values(4, 4, 8)
	r4 := values(66, 5, 3)

	assert.NoError(t,
		p.Add(r1))
	assert.NoError(t,
		p.Add(r2))
	assert.NoError(t,
		p.Add(r3))
	assert.NoError(t,
		p.Add(r4))

	get, err := p.Get(cast(4))
	assert.NoError(t, err)

	assert.Equal(t, [][]sqltypes.Value{r2, r3}, get)
}

func TestKeys(t *testing.T) {
	p := newSingleIntProbeTable(0)
	assert.NoError(t, p.Add(values(1, 2, 6)))
	assert.NoError(t, p.Add(values(1, 4, 3)))
	assert.NoError(t, p.Add(values(4, 4, 8)))
	assert.NoError(t, p.Add(values(66, 5, 3)))

	keys := p.Keys()
	assert.Equal(t, values(1, 4, 66), keys)
}

func values(vals ...int) []sqltypes.Value {
	result := make([]sqltypes.Value, len(vals))
	for i, v := range vals {
		result[i] = cast(v)
	}
	return result
}

func cast(i int) sqltypes.Value {
	return sqltypes.NewInt64(int64(i))
}

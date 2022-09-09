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

package pools

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNumberedGeneral(t *testing.T) {
	id := int64(0)
	p := NewNumbered()

	err := p.Register(id, id)
	require.NoError(t, err)

	err = p.Register(id, id)
	assert.Contains(t, "already present", err.Error())

	var v any
	v, err = p.Get(id, "test")
	require.NoError(t, err)
	assert.Equal(t, id, v.(int64))

	_, err = p.Get(id, "test1")
	assert.Contains(t, "in use: test", err.Error())

	p.Put(id)
	_, err = p.Get(1, "test2")
	assert.Contains(t, "not found", err.Error())
	p.Unregister(1, "test") // Should not fail
	p.Unregister(0, "test")
	// p is now empty

	if _, err = p.Get(0, "test3"); !(strings.HasPrefix(err.Error(), "ended at") && strings.HasSuffix(err.Error(), "(test)")) {
		t.Errorf("want prefix 'ended at' and suffix '(test)', got '%v'", err)
	}

	if p.Size() != 0 {
		t.Errorf("want 0, got %v", p.Size())
	}
	p.WaitForEmpty()
}

func TestNumberedGetByFilter(t *testing.T) {
	p := NewNumbered()
	p.Register(1, 1)
	p.Register(2, 2)
	p.Register(3, 3)
	p.Get(1, "locked")

	vals := p.GetByFilter("filtered", func(v any) bool {
		return v.(int) <= 2
	})
	want := []any{2}
	assert.Equal(t, want, vals)
}

/*
go test --test.run=XXX --test.bench=. --test.benchtime=10s

golang.org/x/tools/cmd/benchcmp /tmp/bad.out /tmp/good.out

benchmark                                 old ns/op     new ns/op     delta
BenchmarkRegisterUnregister-8             667           596           -10.64%
BenchmarkRegisterUnregisterParallel-8     2430          1752          -27.90%
*/
func BenchmarkRegisterUnregister(b *testing.B) {
	p := NewNumbered()
	id := int64(1)
	val := "foobarbazdummyval"
	for i := 0; i < b.N; i++ {
		p.Register(id, val)
		p.Unregister(id, "some reason")
	}
}

func BenchmarkRegisterUnregisterParallel(b *testing.B) {
	p := NewNumbered()
	val := "foobarbazdummyval"
	b.SetParallelism(200)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := rand.Int63()
			p.Register(id, val)
			p.Unregister(id, "some reason")
		}
	})
}

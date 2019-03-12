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

package pools

import (
	"math/rand"
	"strings"
	"testing"
	"time"
)

func TestNumbered(t *testing.T) {
	id := int64(0)
	p := NewNumbered()

	var err error
	if err = p.Register(id, id, true); err != nil {
		t.Errorf("Error %v", err)
	}
	if err = p.Register(id, id, true); err.Error() != "already present" {
		t.Errorf("want 'already present', got '%v'", err)
	}
	var v interface{}
	if v, err = p.Get(id, "test"); err != nil {
		t.Errorf("Error %v", err)
	}
	if v.(int64) != id {
		t.Errorf("want %v, got %v", id, v.(int64))
	}
	if _, err = p.Get(id, "test1"); err.Error() != "in use: test" {
		t.Errorf("want 'in use: test', got '%v'", err)
	}
	p.Put(id)
	if _, err = p.Get(1, "test2"); err.Error() != "not found" {
		t.Errorf("want 'not found', got '%v'", err)
	}
	p.Unregister(1, "test") // Should not fail
	p.Unregister(0, "test")
	// p is now empty

	if _, err = p.Get(0, "test3"); !(strings.HasPrefix(err.Error(), "ended at") && strings.HasSuffix(err.Error(), "(test)")) {
		t.Errorf("want prefix 'ended at' and suffix '(test'), got '%v'", err)
	}

	p.Register(id, id, true)
	id++
	p.Register(id, id, true)
	id++
	p.Register(id, id, false)
	time.Sleep(300 * time.Millisecond)
	id++
	p.Register(id, id, true)
	time.Sleep(100 * time.Millisecond)

	// p has 0, 1, 2, 3 (0, 1, 2 are aged, but 2 is not enforced)
	vals := p.GetOutdated(200*time.Millisecond, "by outdated")
	if num := len(vals); num != 2 {
		t.Errorf("want 2, got %v", num)
	}
	if _, err = p.Get(vals[0].(int64), "test1"); err.Error() != "in use: by outdated" {
		t.Errorf("want 'in use: by outdated', got '%v'", err)
	}
	for _, v := range vals {
		p.Put(v.(int64))
	}
	p.Put(2) // put to 2 to ensure it's not idle
	time.Sleep(100 * time.Millisecond)

	// p has 0, 1, 2 (2 is idle)
	vals = p.GetIdle(200*time.Millisecond, "by idle")
	if len(vals) != 1 {
		t.Errorf("want 1, got %v", len(vals))
	}
	if _, err = p.Get(vals[0].(int64), "test1"); err.Error() != "in use: by idle" {
		t.Errorf("want 'in use: by idle', got '%v'", err)
	}
	if vals[0].(int64) != 3 {
		t.Errorf("want 3, got %v", vals[0])
	}
	p.Unregister(vals[0].(int64), "test")

	// p has 0, 1, and 2
	if p.Size() != 3 {
		t.Errorf("want 3, got %v", p.Size())
	}
	go func() {
		p.Unregister(0, "test")
		p.Unregister(1, "test")
		p.Unregister(2, "test")
	}()
	p.WaitForEmpty()
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
		p.Register(id, val, false)
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
			p.Register(id, val, false)
			p.Unregister(id, "some reason")
		}
	})
}

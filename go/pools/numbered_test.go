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
	"testing"
	"time"
)

func TestNumbered(t *testing.T) {
	id := int64(0)
	p := NewNumbered()

	var err error
	if err = p.Register(id, id); err != nil {
		t.Errorf("Error %v", err)
	}
	if err = p.Register(id, id); err.Error() != "already present" {
		t.Errorf("want 'already present', got '%v'", err)
	}
	var v interface{}
	if v, err = p.Get(id, "test"); err != nil {
		t.Errorf("Error %v", err)
	}
	if v.(int64) != id {
		t.Errorf("want %v, got %v", id, v.(int64))
	}
	if v, err = p.Get(id, "test1"); err.Error() != "in use: test" {
		t.Errorf("want 'in use: test', got '%v'", err)
	}
	p.Put(id)
	if v, err = p.Get(1, "test2"); err.Error() != "not found" {
		t.Errorf("want 'not found', got '%v'", err)
	}
	p.Unregister(1) // Should not fail
	p.Unregister(0)
	// p is now empty

	p.Register(id, id)
	id++
	p.Register(id, id)
	time.Sleep(300 * time.Millisecond)
	id++
	p.Register(id, id)
	time.Sleep(100 * time.Millisecond)

	// p has 0, 1, 2 (0 & 1 are aged)
	vals := p.GetOutdated(200*time.Millisecond, "by outdated")
	if len(vals) != 2 {
		t.Errorf("want 2, got %v", len(vals))
	}
	if v, err = p.Get(vals[0].(int64), "test1"); err.Error() != "in use: by outdated" {
		t.Errorf("want 'in use: by outdated', got '%v'", err)
	}
	for _, v := range vals {
		p.Put(v.(int64))
	}
	time.Sleep(100 * time.Millisecond)

	// p has 0, 1, 2 (2 is idle)
	vals = p.GetIdle(200*time.Millisecond, "by idle")
	if len(vals) != 1 {
		t.Errorf("want 1, got %v", len(vals))
	}
	if v, err = p.Get(vals[0].(int64), "test1"); err.Error() != "in use: by idle" {
		t.Errorf("want 'in use: by idle', got '%v'", err)
	}
	if vals[0].(int64) != 2 {
		t.Errorf("want 2, got %v", vals[0])
	}
	p.Unregister(vals[0].(int64))

	// p has 0 & 1
	if p.Size() != 2 {
		t.Errorf("want 2, got %v", p.Size())
	}
	go func() {
		p.Unregister(0)
		p.Unregister(1)
	}()
	p.WaitForEmpty()
}

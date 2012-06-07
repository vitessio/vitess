// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
		t.Errorf("Expecting 'already present', received '%v'", err)
	}
	var v interface{}
	if v, err = p.Get(id); err != nil {
		t.Errorf("Error %v", err)
	}
	if v.(int64) != id {
		t.Errorf("Expecting %v, received %v", id, v.(int64))
	}
	if v, err = p.Get(id); err.Error() != "in use" {
		t.Errorf("Expecting 'in use', received '%v'", err)
	}
	p.Put(id)
	if v, err = p.Get(1); err.Error() != "not found" {
		t.Errorf("Expecting 'not found', received '%v'", err)
	}
	p.Unregister(1) // Should not fail
	p.Unregister(0)

	p.Register(id, id)
	id++
	p.Register(id, id)
	time.Sleep(3e8)
	id++
	p.Register(id, id)
	time.Sleep(1e8)
	vals := p.GetTimedout(time.Duration(2e8))
	if len(vals) != 2 {
		t.Errorf("Expecting 2, received %v", len(vals))
	}
	for _, v := range vals {
		p.Unregister(v.(int64))
	}

	if p.Stats() != 1 {
		t.Errorf("Expecting 1, received %v", p.Stats())
	}
	go func() {
		p.Unregister(2)
	}()
	p.WaitForEmpty()
}

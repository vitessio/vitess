// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pools

import (
	"errors"
	"testing"
	"time"

	"code.google.com/p/vitess/go/sync2"
)

var lastId, count sync2.AtomicInt64

type TestResource struct {
	num    int64
	closed bool
}

func (tr *TestResource) Close() {
	if !tr.closed {
		count.Add(-1)
		tr.closed = true
	}
}

func (tr *TestResource) IsClosed() bool {
	return tr.closed
}

func PoolFactory() (Resource, error) {
	count.Add(1)
	return &TestResource{lastId.Add(1), false}, nil
}

func FailFactory() (Resource, error) {
	return nil, errors.New("Failed")
}

func SlowFailFactory() (Resource, error) {
	time.Sleep(10 * time.Nanosecond)
	return nil, errors.New("Failed")
}

func TestClosed(t *testing.T) {
	p := NewResourcePool(4, time.Second)
	stats := p.StatsJSON()
	expected := `{"Size": 0, "Capacity": 4, "Available": 0, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}

	// Get returns error when closed
	_, err := p.Get()
	if err == nil {
		t.Errorf("expecting error")
	}

	// TryGet returns error when closed
	_, err = p.TryGet()
	if err == nil {
		t.Errorf("expecting error")
	}

	// Put panics when closed
	func() {
		defer func() {
			if x := recover(); x == nil {
				t.Errorf("expecting panic")
			}
		}()
		p.Put(nil)
	}()

	// SetCapacity is allowed when closed
	p.SetCapacity(5)
	stats = p.StatsJSON()
	expected = `{"Size": 0, "Capacity": 5, "Available": 0, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}

	// Close is allowed when closed
	func() {
		defer func() {
			if x := recover(); x != nil {
				t.Errorf("expecting no panic, got %v", x)
			}
		}()
		p.Close()
	}()

	// Open is allowed when closed
	p.Open(PoolFactory)
	stats = p.StatsJSON()
	expected = `{"Size": 0, "Capacity": 5, "Available": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}
}

func TestOpen(t *testing.T) {
	lastId.Set(0)
	count.Set(0)
	p := NewResourcePool(5, time.Second)
	p.Open(PoolFactory)
	var resources [10]Resource

	// Open is not allowed when OPEN
	func() {
		defer func() {
			if x := recover(); x == nil {
				t.Errorf("expecting panic")
			}
		}()
		p.Open(PoolFactory)
	}()

	// Test Get
	for i := 0; i < 5; i++ {
		r, err := p.Get()
		resources[i] = r
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		_, _, available, waitCount, waitTime, _ := p.Stats()
		if available != int64(5-i-1) {
			t.Errorf("expecting %d, received %d", 5-i-1, available)
		}
		if waitCount != 0 {
			t.Errorf("expecting 0, received %d", waitCount)
		}
		if waitTime != 0 {
			t.Errorf("expecting 0, received %d", waitTime)
		}
		if lastId.Get() != int64(i+1) {
			t.Errorf("Expecting %d, received %d", i+1, lastId.Get())
		}
		if count.Get() != int64(i+1) {
			t.Errorf("Expecting %d, received %d", i+1, count.Get())
		}
	}

	// Test TryGet
	r, err := p.TryGet()
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	if r != nil {
		t.Errorf("Expecting nil")
	}
	for i := 0; i < 5; i++ {
		p.Put(resources[i])
		_, _, available, _, _, _ := p.Stats()
		if available != int64(i+1) {
			t.Errorf("expecting %d, received %d", 5-i-1, available)
		}
	}
	for i := 0; i < 5; i++ {
		r, err := p.TryGet()
		resources[i] = r
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		if r == nil {
			t.Errorf("Expecting non-nil")
		}
		if lastId.Get() != 5 {
			t.Errorf("Expecting 5, received %d", lastId.Get())
		}
		if count.Get() != 5 {
			t.Errorf("Expecting 5, received %d", count.Get())
		}
	}

	// Test that Get waits
	ch := make(chan bool)
	go func() {
		for i := 0; i < 5; i++ {
			r, err := p.Get()
			if err != nil {
				t.Errorf("Get failed: %v", err)
			}
			resources[i] = r
		}
		for i := 0; i < 5; i++ {
			p.Put(resources[i])
		}
		ch <- true
	}()
	for i := 0; i < 5; i++ {
		// Sleep to ensure the goroutine waits
		time.Sleep(10 * time.Nanosecond)
		p.Put(resources[i])
	}
	<-ch
	_, _, _, waitCount, waitTime, _ := p.Stats()
	if waitCount != 5 {
		t.Errorf("Expecting 5, received %d", waitCount)
	}
	if waitTime == 0 {
		t.Errorf("Expecting non-zero")
	}
	if lastId.Get() != 5 {
		t.Errorf("Expecting 5, received %d", lastId.Get())
	}

	// Test Close resource
	r, err = p.Get()
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	r.Close()
	p.Put(r)
	size, _, _, _, _, _ := p.Stats()
	if size != 4 {
		t.Errorf("Expecting 4, received %d", size)
	}
	if count.Get() != 4 {
		t.Errorf("Expecting 4, received %d", count.Get())
	}
	for i := 0; i < 5; i++ {
		r, err := p.Get()
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		resources[i] = r
	}
	for i := 0; i < 5; i++ {
		p.Put(resources[i])
	}
	size, _, _, _, _, _ = p.Stats()
	if size != 5 {
		t.Errorf("Expecting 5, received %d", size)
	}
	if count.Get() != 5 {
		t.Errorf("Expecting 5, received %d", count.Get())
	}
	if lastId.Get() != 6 {
		t.Errorf("Expecting 6, received %d", lastId.Get())
	}

	// SetCapacity
	p.SetCapacity(3)
	if count.Get() != 3 {
		t.Errorf("Expecting 3, received %d", count.Get())
	}
	if lastId.Get() != 6 {
		t.Errorf("Expecting 6, received %d", lastId.Get())
	}
	size, capacity, available, _, _, _ := p.Stats()
	if size != 3 {
		t.Errorf("Expecting 3, received %d", size)
	}
	if capacity != 3 {
		t.Errorf("Expecting 3, received %d", capacity)
	}
	if available != 3 {
		t.Errorf("Expecting 3, received %d", available)
	}
	p.SetCapacity(6)
	size, capacity, available, _, _, _ = p.Stats()
	if size != 3 {
		t.Errorf("Expecting 3, received %d", size)
	}
	if capacity != 6 {
		t.Errorf("Expecting 6, received %d", capacity)
	}
	if available != 6 {
		t.Errorf("Expecting 6, received %d", available)
	}
	for i := 0; i < 6; i++ {
		r, err := p.Get()
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		resources[i] = r
	}
	for i := 0; i < 6; i++ {
		p.Put(resources[i])
	}
	if count.Get() != 6 {
		t.Errorf("Expecting 5, received %d", count.Get())
	}
	if lastId.Get() != 9 {
		t.Errorf("Expecting 9, received %d", lastId.Get())
	}

	// Close
	p.Close()
	size, capacity, available, _, _, _ = p.Stats()
	if size != 0 {
		t.Errorf("Expecting 0, received %d", size)
	}
	if capacity != 6 {
		t.Errorf("Expecting 6, received %d", capacity)
	}
	if available != 0 {
		t.Errorf("Expecting 0, received %d", available)
	}
	if count.Get() != 0 {
		t.Errorf("Expecting 0, received %d", count.Get())
	}
}

func TestShrinking(t *testing.T) {
	lastId.Set(0)
	count.Set(0)
	p := NewResourcePool(5, time.Second)
	p.Open(PoolFactory)
	var resources [10]Resource
	// Leave one empty slot in the pool
	for i := 0; i < 4; i++ {
		r, err := p.Get()
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		resources[i] = r
	}
	go p.SetCapacity(3)
	time.Sleep(10 * time.Nanosecond)
	stats := p.StatsJSON()
	expected := `{"Size": 4, "Capacity": 5, "Available": 0, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}

	// TryGet is allowed when shrinking
	r, err := p.TryGet()
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	if r != nil {
		t.Errorf("Expecting nil")
	}

	// Get is allowed when shrinking, but it will wait
	getdone := make(chan bool)
	go func() {
		r, err := p.Get()
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		p.Put(r)
		getdone <- true
	}()

	// Put is allowed when shrinking. It's necessary.
	for i := 0; i < 4; i++ {
		p.Put(resources[i])
	}
	// Wait for Get test to complete
	<-getdone
	stats = p.StatsJSON()
	expected = `{"Size": 3, "Capacity": 3, "Available": 3, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}
	if count.Get() != 3 {
		t.Errorf("Expecting 3, received %d", count.Get())
	}
}

func TestClosing(t *testing.T) {
	lastId.Set(0)
	count.Set(0)
	p := NewResourcePool(5, time.Second)
	p.Open(PoolFactory)
	var resources [10]Resource
	for i := 0; i < 5; i++ {
		r, err := p.Get()
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		resources[i] = r
	}
	ch := make(chan bool)
	go func() {
		p.Close()
		ch <- true
	}()

	// Wait for goroutine to call Close
	time.Sleep(10 * time.Nanosecond)
	stats := p.StatsJSON()
	expected := `{"Size": 5, "Capacity": 5, "Available": 0, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}

	// TryGet is not allowed when closing
	_, err := p.TryGet()
	if err == nil {
		t.Errorf("expecting error")
	}

	// Get is not allowed when closing
	_, err = p.Get()
	if err == nil {
		t.Errorf("expecting error")
	}

	// Put is allowed when closing
	for i := 0; i < 5; i++ {
		p.Put(resources[i])
	}

	// Wait for Close to return
	<-ch
	stats = p.StatsJSON()
	expected = `{"Size": 0, "Capacity": 5, "Available": 0, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}
	if lastId.Get() != 5 {
		t.Errorf("Expecting 5, received %d", count.Get())
	}
	if count.Get() != 0 {
		t.Errorf("Expecting 0, received %d", count.Get())
	}
}

func TestIdleTimeout(t *testing.T) {
	lastId.Set(0)
	count.Set(0)
	p := NewResourcePool(1, 10*time.Nanosecond)
	p.Open(PoolFactory)
	defer p.Close()

	r, err := p.Get()
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	p.Put(r)
	if lastId.Get() != 1 {
		t.Errorf("Expecting 1, received %d", count.Get())
	}
	if count.Get() != 1 {
		t.Errorf("Expecting 1, received %d", count.Get())
	}
	time.Sleep(20 * time.Nanosecond)
	r, err = p.Get()
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	if lastId.Get() != 2 {
		t.Errorf("Expecting 2, received %d", count.Get())
	}
	if count.Get() != 1 {
		t.Errorf("Expecting 1, received %d", count.Get())
	}
	p.Put(r)
}

func TestCreateFail(t *testing.T) {
	lastId.Set(0)
	count.Set(0)
	p := NewResourcePool(5, time.Second)
	p.Open(FailFactory)
	defer p.Close()
	if _, err := p.Get(); err.Error() != "Failed" {
		t.Errorf("Expecting Failed, received %c", err)
	}
	stats := p.StatsJSON()
	expected := `{"Size": 0, "Capacity": 5, "Available": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}
}

func TestSlowCreateFail(t *testing.T) {
	lastId.Set(0)
	count.Set(0)
	p := NewResourcePool(2, time.Second)
	p.Open(SlowFailFactory)
	defer p.Close()
	ch := make(chan bool)
	// The third Get should not wait indefinitely
	for i := 0; i < 3; i++ {
		go func() {
			p.Get()
			ch <- true
		}()
	}
	for i := 0; i < 3; i++ {
		<-ch
	}
	size, _, available, _, _, _ := p.Stats()
	if size != 0 {
		t.Errorf("Expecting 2, received %d", size)
	}
	if available != 2 {
		t.Errorf("Expecting 2, received %d", available)
	}
}

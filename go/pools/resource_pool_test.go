// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pools

import (
	"errors"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sync2"
	"golang.org/x/net/context"
)

var lastID, count sync2.AtomicInt64

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

func PoolFactory() (Resource, error) {
	count.Add(1)
	return &TestResource{lastID.Add(1), false}, nil
}

func FailFactory() (Resource, error) {
	return nil, errors.New("Failed")
}

func SlowFailFactory() (Resource, error) {
	time.Sleep(10 * time.Nanosecond)
	return nil, errors.New("Failed")
}

func TestOpen(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewResourcePool(PoolFactory, 6, 6, time.Second)
	p.SetCapacity(5)
	var resources [10]Resource

	// Test Get
	for i := 0; i < 5; i++ {
		r, err := p.Get(ctx)
		resources[i] = r
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		_, available, _, waitCount, waitTime, _ := p.Stats()
		if available != int64(5-i-1) {
			t.Errorf("expecting %d, received %d", 5-i-1, available)
		}
		if waitCount != 0 {
			t.Errorf("expecting 0, received %d", waitCount)
		}
		if waitTime != 0 {
			t.Errorf("expecting 0, received %d", waitTime)
		}
		if lastID.Get() != int64(i+1) {
			t.Errorf("Expecting %d, received %d", i+1, lastID.Get())
		}
		if count.Get() != int64(i+1) {
			t.Errorf("Expecting %d, received %d", i+1, count.Get())
		}
	}

	// Test that Get waits
	ch := make(chan bool)
	go func() {
		for i := 0; i < 5; i++ {
			r, err := p.Get(ctx)
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
		time.Sleep(10 * time.Millisecond)
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
	if lastID.Get() != 5 {
		t.Errorf("Expecting 5, received %d", lastID.Get())
	}

	// Test Close resource
	r, err := p.Get(ctx)
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	r.Close()
	p.Put(nil)
	if count.Get() != 4 {
		t.Errorf("Expecting 4, received %d", count.Get())
	}
	for i := 0; i < 5; i++ {
		r, err := p.Get(ctx)
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		resources[i] = r
	}
	for i := 0; i < 5; i++ {
		p.Put(resources[i])
	}
	if count.Get() != 5 {
		t.Errorf("Expecting 5, received %d", count.Get())
	}
	if lastID.Get() != 6 {
		t.Errorf("Expecting 6, received %d", lastID.Get())
	}

	// SetCapacity
	p.SetCapacity(3)
	if count.Get() != 3 {
		t.Errorf("Expecting 3, received %d", count.Get())
	}
	if lastID.Get() != 6 {
		t.Errorf("Expecting 6, received %d", lastID.Get())
	}
	capacity, available, _, _, _, _ := p.Stats()
	if capacity != 3 {
		t.Errorf("Expecting 3, received %d", capacity)
	}
	if available != 3 {
		t.Errorf("Expecting 3, received %d", available)
	}
	p.SetCapacity(6)
	capacity, available, _, _, _, _ = p.Stats()
	if capacity != 6 {
		t.Errorf("Expecting 6, received %d", capacity)
	}
	if available != 6 {
		t.Errorf("Expecting 6, received %d", available)
	}
	for i := 0; i < 6; i++ {
		r, err := p.Get(ctx)
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
	if lastID.Get() != 9 {
		t.Errorf("Expecting 9, received %d", lastID.Get())
	}

	// Close
	p.Close()
	capacity, available, _, _, _, _ = p.Stats()
	if capacity != 0 {
		t.Errorf("Expecting 0, received %d", capacity)
	}
	if available != 0 {
		t.Errorf("Expecting 0, received %d", available)
	}
	if count.Get() != 0 {
		t.Errorf("Expecting 0, received %d", count.Get())
	}
}

func TestShrinking(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewResourcePool(PoolFactory, 5, 5, time.Second)
	var resources [10]Resource
	// Leave one empty slot in the pool
	for i := 0; i < 4; i++ {
		r, err := p.Get(ctx)
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		resources[i] = r
	}
	done := make(chan bool)
	go func() {
		p.SetCapacity(3)
		done <- true
	}()
	expected := `{"Capacity": 3, "Available": 0, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Millisecond)
		stats := p.StatsJSON()
		if stats != expected {
			if i == 9 {
				t.Errorf(`expecting '%s', received '%s'`, expected, stats)
			}
		}
	}
	// There are already 2 resources available in the pool.
	// So, returning one should be enough for SetCapacity to complete.
	p.Put(resources[3])
	<-done
	// Return the rest of the resources
	for i := 0; i < 3; i++ {
		p.Put(resources[i])
	}
	stats := p.StatsJSON()
	expected = `{"Capacity": 3, "Available": 3, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}
	if count.Get() != 3 {
		t.Errorf("Expecting 3, received %d", count.Get())
	}

	// Ensure no deadlock if SetCapacity is called after we start
	// waiting for a resource
	var err error
	for i := 0; i < 3; i++ {
		resources[i], err = p.Get(ctx)
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
	}
	// This will wait because pool is empty
	go func() {
		r, err := p.Get(ctx)
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		p.Put(r)
		done <- true
	}()

	// This will also wait
	go func() {
		p.SetCapacity(2)
		done <- true
	}()
	time.Sleep(10 * time.Millisecond)

	// This should not hang
	for i := 0; i < 3; i++ {
		p.Put(resources[i])
	}
	<-done
	<-done
	capacity, available, _, waitcount, _, _ := p.Stats()
	if capacity != 2 {
		t.Errorf("Expecting 2, received %d", capacity)
	}
	if available != 2 {
		t.Errorf("Expecting 2, received %d", available)
	}
	if waitcount != 1 {
		t.Errorf("Expecting 1, received %d", waitcount)
	}
	if count.Get() != 2 {
		t.Errorf("Expecting 2, received %d", count.Get())
	}

	// Test race condition of SetCapacity with itself
	p.SetCapacity(3)
	for i := 0; i < 3; i++ {
		resources[i], err = p.Get(ctx)
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
	}
	// This will wait because pool is empty
	go func() {
		r, err := p.Get(ctx)
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		p.Put(r)
		done <- true
	}()
	time.Sleep(10 * time.Nanosecond)

	// This will wait till we Put
	go p.SetCapacity(2)
	time.Sleep(10 * time.Nanosecond)
	go p.SetCapacity(4)
	time.Sleep(10 * time.Nanosecond)

	// This should not hang
	for i := 0; i < 3; i++ {
		p.Put(resources[i])
	}
	<-done

	err = p.SetCapacity(-1)
	if err == nil {
		t.Errorf("Expecting error")
	}
	err = p.SetCapacity(255555)
	if err == nil {
		t.Errorf("Expecting error")
	}

	capacity, available, _, _, _, _ = p.Stats()
	if capacity != 4 {
		t.Errorf("Expecting 4, received %d", capacity)
	}
	if available != 4 {
		t.Errorf("Expecting 4, received %d", available)
	}
}

func TestClosing(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewResourcePool(PoolFactory, 5, 5, time.Second)
	var resources [10]Resource
	for i := 0; i < 5; i++ {
		r, err := p.Get(ctx)
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
	expected := `{"Capacity": 0, "Available": 0, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}

	// Put is allowed when closing
	for i := 0; i < 5; i++ {
		p.Put(resources[i])
	}

	// Wait for Close to return
	<-ch

	// SetCapacity must be ignored after Close
	err := p.SetCapacity(1)
	if err == nil {
		t.Errorf("expecting error")
	}

	stats = p.StatsJSON()
	expected = `{"Capacity": 0, "Available": 0, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}
	if lastID.Get() != 5 {
		t.Errorf("Expecting 5, received %d", count.Get())
	}
	if count.Get() != 0 {
		t.Errorf("Expecting 0, received %d", count.Get())
	}
}

func TestIdleTimeout(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewResourcePool(PoolFactory, 1, 1, 10*time.Nanosecond)
	defer p.Close()

	r, err := p.Get(ctx)
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	p.Put(r)
	if lastID.Get() != 1 {
		t.Errorf("Expecting 1, received %d", count.Get())
	}
	if count.Get() != 1 {
		t.Errorf("Expecting 1, received %d", count.Get())
	}
	time.Sleep(20 * time.Nanosecond)
	r, err = p.Get(ctx)
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	if lastID.Get() != 2 {
		t.Errorf("Expecting 2, received %d", count.Get())
	}
	if count.Get() != 1 {
		t.Errorf("Expecting 1, received %d", count.Get())
	}
	p.Put(r)
}

func TestCreateFail(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewResourcePool(FailFactory, 5, 5, time.Second)
	defer p.Close()
	if _, err := p.Get(ctx); err.Error() != "Failed" {
		t.Errorf("Expecting Failed, received %v", err)
	}
	stats := p.StatsJSON()
	expected := `{"Capacity": 5, "Available": 5, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000}`
	if stats != expected {
		t.Errorf(`expecting '%s', received '%s'`, expected, stats)
	}
}

func TestSlowCreateFail(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewResourcePool(SlowFailFactory, 2, 2, time.Second)
	defer p.Close()
	ch := make(chan bool)
	// The third Get should not wait indefinitely
	for i := 0; i < 3; i++ {
		go func() {
			p.Get(ctx)
			ch <- true
		}()
	}
	for i := 0; i < 3; i++ {
		<-ch
	}
	_, available, _, _, _, _ := p.Stats()
	if available != 2 {
		t.Errorf("Expecting 2, received %d", available)
	}
}

func TestTimeout(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewResourcePool(PoolFactory, 1, 1, time.Second)
	defer p.Close()
	r, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	newctx, cancel := context.WithTimeout(ctx, 1*time.Millisecond)
	_, err = p.Get(newctx)
	cancel()
	want := "resource pool timed out"
	if err == nil || err.Error() != want {
		t.Errorf("got %v, want %s", err, want)
	}
	p.Put(r)
}

func TestExpired(t *testing.T) {
	lastID.Set(0)
	count.Set(0)
	p := NewResourcePool(PoolFactory, 1, 1, time.Second)
	defer p.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Second))
	r, err := p.Get(ctx)
	if err == nil {
		p.Put(r)
	}
	cancel()
	want := "resource pool timed out"
	if err == nil || err.Error() != want {
		t.Errorf("got %v, want %s", err, want)
	}
}

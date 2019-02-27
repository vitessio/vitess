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
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"golang.org/x/net/context"
)

func SlowCreateFactory() (Resource, error) {
	time.Sleep(10 * time.Millisecond)
	return PoolFactory()
}

type SlowCloseResource struct {
	num    int64
	closed bool
}

func (r *SlowCloseResource) Close() {
	if !r.closed {
		time.Sleep(10 * time.Millisecond)
		count.Add(-1)
		r.closed = true
	}
}

func SlowCloseFactory() (Resource, error) {
	count.Add(1)
	return &SlowCloseResource{lastID.Add(1), false}, nil
}

func get(t *testing.T, p *NewPool) Resource {
	t.Helper()
	r, err := p.Get(context.Background())
	require.NoError(t, err)
	return r
}

func TestNewOpen(t *testing.T) {
	p := NewNewPool(PoolFactory, 1, 2, 0, 0)
	defer p.Close()

	require.Equal(t, State{Capacity: 1}, p.State())
}

func TestNewGetPut(t *testing.T) {
	lastID.Set(0)
	p := NewNewPool(PoolFactory, 1, 1, 0, 0)
	defer p.Close()

	r := get(t, p).(*TestResource)
	require.Equal(t, r.num, int64(1))
	require.Equal(t, State{Capacity: 1, InUse: 1}, p.State())

	p.Put(r)
	require.Equal(t, State{Capacity: 1, InPool: 1}, p.State())
}

func TestNewPutEmpty(t *testing.T) {
	p := NewNewPool(PoolFactory, 1, 1, 0, 0)
	defer p.Close()

	get(t, p)
	p.Put(nil)

	require.Equal(t, len(p.pool), 0)
	require.Equal(t, State{Capacity: 1}, p.State())
}

func TestNewPutWithoutGet(t *testing.T) {
	p := NewNewPool(PoolFactory, 1, 1, 0, 0)
	defer p.Close()

	require.Panics(t, func() { p.Put(&TestResource{}) })
	require.Equal(t, State{Capacity: 1}, p.State())
}

func TestNewPutTooFull(t *testing.T) {
	p := NewNewPool(PoolFactory, 1, 1, 0, 1)
	defer p.Close()

	// Not sure how to cause the ErrFull panic naturally, so I'm hacking in a value.
	p.state.InUse = 1

	require.Panics(t, func() { p.Put(&TestResource{}) })
	require.Equal(t, State{Capacity: 1, MinActive: 1, InPool: 1, InUse: 1}, p.State())

	// Allow p.Close() to not stall on a non-existent resource.
	p.state.InUse = 0
}

func TestNewDrainBlock(t *testing.T) {
	p := NewNewPool(SlowCloseFactory, 3, 3, 0, 0)
	defer p.Close()

	var resources []Resource
	for i := 0; i < 3; i++ {
		resources = append(resources, get(t, p))
	}
	p.Put(resources[0])
	require.Equal(t, State{Capacity: 3, InUse: 2, InPool: 1}, p.State())

	done := make(chan bool)
	go func() {
		require.NoError(t, p.SetCapacity(1, true))
		done <- true
	}()

	time.Sleep(time.Millisecond * 30)
	// The first resource should be closed by now, but waiting for the second to unblock.
	require.Equal(t, State{Capacity: 1, Draining: true, InUse: 2}, p.State())

	p.Put(resources[1])

	<-done
	require.Equal(t, State{Capacity: 1, InUse: 1}, p.State())

	// Clean up so p.Close() can run properly.
	p.Put(resources[2])
}

func TestNewDrainNoBlock(t *testing.T) {
	p := NewNewPool(SlowCloseFactory, 2, 2, 0, 0)
	defer p.Close()

	var resources []Resource
	for i := 0; i < 2; i++ {
		resources = append(resources, get(t, p))
	}
	p.Put(resources[0])
	require.Equal(t, State{Capacity: 2, InUse: 1, InPool: 1}, p.State())

	require.NoError(t, p.SetCapacity(1, false))
	require.Equal(t, State{Capacity: 1, Draining: true, InUse: 1, InPool: 1}, p.State())

	p.Put(resources[1])

	// Wait until the resources close, at least 20ms (2 * 10ms per SlowResource)
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, State{Capacity: 1, InPool: 1}, p.State())
}

func TestNewGrowBlock(t *testing.T) {
	p := NewNewPool(PoolFactory, 1, 2, 0, 0)
	defer p.Close()

	a := get(t, p)
	require.Equal(t, State{Capacity: 1, InUse: 1}, p.State())
	require.NoError(t, p.SetCapacity(2, true))
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 2, InUse: 1}, p.State())
	b := get(t, p)
	require.Equal(t, State{Capacity: 2, InUse: 2}, p.State())

	// Clean up
	p.Put(a)
	p.Put(b)
}

func TestNewGrowNoBlock(t *testing.T) {
	p := NewNewPool(PoolFactory, 1, 2, 0, 0)
	defer p.Close()

	a := get(t, p)
	require.Equal(t, State{Capacity: 1, InUse: 1}, p.State())
	require.NoError(t, p.SetCapacity(2, false))
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 2, InUse: 1}, p.State())
	b := get(t, p)
	require.Equal(t, State{Capacity: 2, InUse: 2}, p.State())

	// Clean up
	p.Put(a)
	p.Put(b)
}

func TestNewFull1(t *testing.T) {
	s := func(state State) State {
		state.Capacity = 10
		state.MinActive = 5
		return state
	}

	p := NewNewPool(PoolFactory, 10, 10, 0, 5)
	defer p.Close()

	require.Equal(t, s(State{InPool: 5}), p.State())

	a := get(t, p)
	require.Equal(t, s(State{InPool: 4, InUse: 1}), p.State())

	var resources []Resource
	for i := 0; i < 9; i++ {
		resources = append(resources, get(t, p))
	}
	require.Equal(t, s(State{InPool: 0, InUse: 10}), p.State())

	var b Resource
	done := make(chan bool)
	go func() {
		t.Helper()
		b = get(t, p)
		done <- true
	}()
	time.Sleep(time.Millisecond)
	require.Equal(t, s(State{InPool: 0, InUse: 10, Waiters: 1}), p.State())

	fmt.Println("simpl4")

	p.Put(a)
	fmt.Println("simpl4a")
	<-done
	fmt.Println("simpl4b")
	require.NotZero(t, p.State().WaitTime)
	wt := p.State().WaitTime
	require.Equal(t, s(State{InPool: 0, InUse: 10, Waiters: 0, WaitCount: 1, WaitTime: wt}), p.State())

	fmt.Println("simpl5")
	p.Put(b)
	require.Equal(t, s(State{InPool: 1, InUse: 9, Waiters: 0, WaitCount: 1, WaitTime: wt}), p.State())
	fmt.Println("simpl6")

	// Clean up
	for _, r := range resources {
		p.Put(r)
	}
}

func TestNewFull2(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewNewPool(PoolFactory, 6, 6, time.Second, 0)
	defer p.Close()

	fmt.Println("set cap", p.StatsJSON())
	err := p.SetCapacity(5, true)
	require.NoError(t, err)
	var resources [10]Resource

	fmt.Println(1, p.StatsJSON())

	// TestNew Get
	for i := 0; i < 5; i++ {
		fmt.Println("\nTestNew Get ----------", i, p.StatsJSON())
		r, err := p.Get(ctx)
		fmt.Println(1, p.StatsJSON())
		resources[i] = r
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		if p.Available() != 5-i-1 {
			t.Errorf("expecting %d, received %d", 5-i-1, p.Available())
			return
		}
		if p.WaitCount() != 0 {
			t.Errorf("expecting 0, received %d", p.WaitCount())
			return
		}
		if p.WaitTime() != 0 {
			t.Errorf("expecting 0, received %d", p.WaitTime())
		}
		if lastID.Get() != int64(i+1) {
			t.Errorf("Expecting %d, received %d", i+1, lastID.Get())
		}
		if count.Get() != int64(i+1) {
			t.Errorf("Expecting %d, received %d", i+1, count.Get())
		}
	}

	fmt.Println("WTFFFFFFFFFF", p.StatsJSON())

	// TestNew that Get waits
	ch := make(chan bool)
	go func() {
		for i := 0; i < 5; i++ {
			fmt.Println("GET", i, p.StatsJSON())
			r, err := p.Get(ctx)
			if err != nil {
				t.Errorf("Get failed: %v", err)
				panic("exit")
			}
			resources[i] = r
		}
		for i := 0; i < 5; i++ {
			fmt.Println("PUT")
			p.Put(resources[i])
		}
		ch <- true
	}()
	for i := 0; i < 5; i++ {
		// Sleep to ensure the goroutine waits
		fmt.Println("SLEEP")
		time.Sleep(10 * time.Millisecond)
		fmt.Println("PUTBACK")
		p.Put(resources[i])
	}
	<-ch
	if p.WaitCount() != 5 {
		t.Errorf("Expecting 5, received %d", p.WaitCount())
	}
	if p.WaitTime() == 0 {
		t.Errorf("Expecting non-zero")
	}
	if lastID.Get() != 5 {
		t.Errorf("Expecting 5, received %d", lastID.Get())
	}

	// TestNew Close resource
	r, err := p.Get(ctx)
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	r.Close()

	fmt.Println(2)

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
	fmt.Println(3)

	// SetCapacity
	p.SetCapacity(3, true)
	if count.Get() != 3 {
		t.Errorf("Expecting 3, received %d", count.Get())
		return
	}
	if lastID.Get() != 6 {
		t.Errorf("Expecting 6, received %d", lastID.Get())
	}
	if p.Capacity() != 3 {
		t.Errorf("Expecting 3, received %d", p.Capacity())
	}
	if p.Available() != 3 {
		t.Errorf("Expecting 3, received %d", p.Available())
	}
	p.SetCapacity(6, true)
	if p.Capacity() != 6 {
		t.Errorf("Expecting 6, received %d", p.Capacity())
	}
	if p.Available() != 6 {
		t.Errorf("Expecting 6, received %d", p.Available())
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
	if p.Capacity() != 0 {
		t.Errorf("Expecting 0, received %d", p.Capacity())
	}
	if p.Available() != 0 {
		t.Errorf("Expecting 0, received %d", p.Available())
	}
	if count.Get() != 0 {
		t.Errorf("Expecting 0, received %d", count.Get())
	}
}

func TestNewShrinking(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewNewPool(PoolFactory, 5, 5, time.Second, 0)
	var resources [10]Resource
	// Leave one empty slot in the pool
	for i := 0; i < 4; i++ {
		resources[i] = get(t, p)
	}

	done := make(chan bool)
	go func() {
		t.Helper()
		require.NoError(t, p.SetCapacity(3, true))
		done <- true
	}()
	expected := `{"Capacity": 3, "Available": 0, "Active": 4, "InUse": 4, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000, "IdleClosed": 0}`
	time.Sleep(time.Millisecond)
	stats := p.StatsJSON()
	require.Equal(t, expected, stats)

	// There are already 2 resources available in the pool.
	// So, returning one should be enough for SetCapacity to complete.
	p.Put(resources[3])
	<-done

	time.Sleep(time.Millisecond * 10)

	fmt.Printf("1>>>>>>>>>>> %+v\n", p.State())
	// {Waiters:0 InPool:1 InUse:3 Capacity:3 MinActive:0 IdleTimeout:1s IdleClosed:0 WaitCount:0 WaitTime:0s}

	// Return the rest of the resources
	for i := 0; i < 3; i++ {
		p.Put(resources[i])
	}
	time.Sleep(time.Millisecond * 10)
	fmt.Printf("2>>>>>>>>>>> %+v\n", p.State())
	// {Waiters:0 InPool:4 InUse:0 Capacity:3 MinActive:0 IdleTimeout:1s IdleClosed:0 WaitCount:0 WaitTime:0s}

	stats = p.StatsJSON()
	expected = `{"Capacity": 3, "Available": 3, "Active": 3, "InUse": 0, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000, "IdleClosed": 0}`
	// TODO: Something is odd here.
	require.Equal(t, expected, stats)
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
		p.SetCapacity(2, true)
		done <- true
	}()
	time.Sleep(10 * time.Millisecond)

	// This should not hang
	for i := 0; i < 3; i++ {
		p.Put(resources[i])
	}
	<-done
	<-done
	time.Sleep(time.Millisecond)
	if p.Capacity() != 2 {
		t.Errorf("Expecting 2, received %d", p.Capacity())
	}
	if p.Available() != 2 {
		t.Errorf("Expecting 2, received %d", p.Available())
	}
	if p.WaitCount() != 1 {
		t.Errorf("Expecting 1, received %d", p.WaitCount())
		return
	}
	if count.Get() != 2 {
		t.Errorf("Expecting 2, received %d", count.Get())
	}

	// TestNew race condition of SetCapacity with itself
	p.SetCapacity(3, true)
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
	time.Sleep(10 * time.Millisecond)

	// This will wait till we Put
	go p.SetCapacity(2, true)
	time.Sleep(10 * time.Millisecond)
	go p.SetCapacity(4, true)
	time.Sleep(10 * time.Millisecond)

	// This should not hang
	for i := 0; i < 3; i++ {
		p.Put(resources[i])
	}
	<-done

	err = p.SetCapacity(-1, true)
	if err == nil {
		t.Errorf("Expecting error")
	}
	err = p.SetCapacity(255555, true)
	time.Sleep(time.Millisecond)
	if err == nil {
		t.Errorf("Expecting error")
	}

	if p.Capacity() != 4 {
		t.Errorf("Expecting 4, received %d", p.Capacity())
	}
	if p.Available() != 4 {
		t.Errorf("Expecting 4, received %d", p.Available())
	}
}

func TestNewClosing(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewNewPool(PoolFactory, 5, 5, time.Second, 0)
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
	time.Sleep(10 * time.Millisecond)
	stats := p.StatsJSON()
	expected := `{"Capacity": 0, "Available": 0, "Active": 5, "InUse": 5, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 0, "IdleClosed": 0}`
	require.Equal(t, expected, stats)

	// Put is allowed when closing
	for i := 0; i < 5; i++ {
		p.Put(resources[i])
	}

	// Wait for Close to return
	<-ch

	// SetCapacity must be ignored after Close
	err := p.SetCapacity(1, true)
	if err == nil {
		t.Errorf("expecting error")
	}

	stats = p.StatsJSON()
	expected = `{"Capacity": 0, "Available": 0, "Active": 0, "InUse": 0, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 0, "IdleClosed": 0}`
	require.Equal(t, expected, stats)
	require.Equal(t, 5, int(lastID.Get()))
	require.Equal(t, 0, int(count.Get()))
}

func TestNewIdleTimeout(t *testing.T) {
	lastID.Set(0)
	count.Set(0)
	p := NewNewPool(PoolFactory, 1, 1, 10*time.Millisecond, 0)
	defer p.Close()

	r := get(t, p)
	require.Equal(t, 1, int(count.Get()))
	require.Equal(t, 0, int(p.IdleClosed()))

	p.Put(r)
	require.Equal(t, 1, int(count.Get()))
	require.Equal(t, 0, int(p.IdleClosed()))

	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 0, int(count.Get()))
	require.Equal(t, 1, int(p.IdleClosed()))

	r = get(t, p)
	require.Equal(t, 2, int(lastID.Get()))
	require.Equal(t, 1, int(count.Get()))
	require.Equal(t, 1, int(p.IdleClosed()))

	// Sleep to let the idle closer run while all resources are in use
	// then make sure things are still as we expect.
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 2, int(lastID.Get()))
	require.Equal(t, 1, int(count.Get()))
	require.Equal(t, 1, int(p.IdleClosed()))

	p.Put(r)
	r = get(t, p)
	require.Equal(t, 2, int(lastID.Get()))
	require.Equal(t, 1, int(count.Get()))
	require.Equal(t, 1, int(p.IdleClosed()))

	// The idle close thread wakes up every 1/10 of the idle time, so ensure
	// the timeout change applies to newly added resources.
	fmt.Println("new long timeout")
	p.SetIdleTimeout(200 * time.Millisecond)
	fmt.Println("new long timeout set")
	p.Put(r)
	fmt.Println("new long timeout put")

	time.Sleep(20 * time.Millisecond)
	fmt.Println("new long timeout after sleep")
	require.Equal(t, 2, int(lastID.Get()))
	require.Equal(t, 1, int(count.Get()))
	require.Equal(t, 1, int(p.IdleClosed()))

	p.SetIdleTimeout(10 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 2, int(lastID.Get()))
	require.Equal(t, 0, int(count.Get()))
	require.Equal(t, 2, int(p.IdleClosed()))
}

func TestNewCreateFail(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewNewPool(FailFactory, 5, 5, time.Second, 0)
	defer p.Close()

	if _, err := p.Get(ctx); err.Error() != "Failed" {
		t.Errorf("Expecting Failed, received %v", err)
	}
	time.Sleep(time.Millisecond)
	stats := p.StatsJSON()
	expected := `{"Capacity": 5, "Available": 5, "Active": 0, "InUse": 0, "MaxCapacity": 5, "WaitCount": 0, "WaitTime": 0, "IdleTimeout": 1000000000, "IdleClosed": 0}`
	require.Equal(t, expected, stats)
}

func TestNewSlowCreateFail(t *testing.T) {
	lastID.Set(0)
	count.Set(0)
	p := NewNewPool(SlowFailFactory, 2, 2, time.Second, 0)
	defer p.Close()

	ch := make(chan bool)
	// The third Get should not wait indefinitely
	for i := 0; i < 3; i++ {
		go func() {
			_, err := p.Get(context.Background())
			require.Error(t, err)
			ch <- true
		}()
	}
	for i := 0; i < 3; i++ {
		<-ch
	}
	time.Sleep(time.Millisecond)
	require.Equal(t, p.Available(), 2)
}

func TestNewTimeoutSlow(t *testing.T) {
	ctx := context.Background()
	p := NewNewPool(SlowCreateFactory, 4, 4, time.Second, 0)

	defer p.Close()

	var resources []Resource
	for i := 0; i < 4; i++ {
		resources = append(resources, get(t, p))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Millisecond*5))
	r, err := p.Get(ctx)
	require.EqualError(t, err, "resource pool timed out")
	cancel()

	expected := State{Capacity: 4, InUse: 4, IdleTimeout: time.Second}
	require.Equal(t, expected, p.State())

	for _, r = range resources {
		p.Put(r)
	}
}

func TestNewTimeoutPoolFull(t *testing.T) {
	ctx := context.Background()
	p := NewNewPool(PoolFactory, 1, 1, time.Second, 0)

	var a, b Resource
	defer func() {
		if a != nil {
			p.Put(a)
		}
		if b != nil {
			p.Put(b)
		}

		p.Close()
	}()

	b = get(t, p)
	expected := State{Capacity: 1, InUse: 1, IdleTimeout: time.Second}
	require.Equal(t, expected, p.State())

	newctx, cancel := context.WithTimeout(ctx, time.Millisecond*50)
	a, err := p.Get(newctx)
	a = nil
	cancel()
	require.EqualError(t, err, "resource pool timed out")

	time.Sleep(time.Millisecond * 10)
	require.Equal(t, expected, p.State())

	p.Put(b)
	b = nil

	expected.InUse = 0
	expected.InPool = 1
	require.Equal(t, expected, p.State())
}

func TestNewExpired(t *testing.T) {
	lastID.Set(0)
	count.Set(0)
	p := NewNewPool(PoolFactory, 1, 1, time.Second, 0)
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

func TestNewMinActive(t *testing.T) {
	lastID.Set(0)
	count.Set(0)
	p := NewNewPool(PoolFactory, 5, 5, time.Second, 3)
	defer p.Close()

	if p.Available() != 5 {
		t.Errorf("Expecting 5, received %d", p.Available())
	}
	if p.Active() != 3 {
		t.Errorf("Expecting 3, received %d", p.Active())
	}
	if lastID.Get() != 3 {
		t.Errorf("Expecting 3, received %d", lastID.Get())
	}
	if count.Get() != 3 {
		t.Errorf("Expecting 3, received %d", count.Get())
	}
}

func TestNewMinActiveWithExpiry(t *testing.T) {
	timeout := time.Millisecond * 20
	lastID.Set(0)
	count.Set(0)
	p := NewNewPool(PoolFactory, 5, 5, timeout, 3)
	defer p.Close()

	require.Equal(t, 3, p.Active())

	expected := State{Capacity: 5, MinActive: 3, IdleTimeout: timeout}

	if count.Get() != 3 {
		t.Errorf("Expecting 3, received %d", count.Get())
		return
	}

	// Get one more than the minActive.
	var resources []Resource
	for i := 0; i < 4; i++ {
		r, err := p.Get(context.Background())
		if err != nil {
			t.Errorf("Got an unexpected error: %v", err)
		}
		resources = append(resources, r)
	}

	// Should have only ever allocated 4 (3 min, 1 extra).
	expected.InUse = 4
	require.Equal(t, expected, p.State())
	require.Equal(t, 4, p.Active())
	require.Equal(t, 4, int(count.Get()))

	// Put one resource back and let it expire.
	p.Put(resources[0])
	time.Sleep(timeout * 2)

	expected.InPool = 0
	expected.InUse = 3
	expected.IdleClosed = 1
	require.Equal(t, expected, p.State())
	require.Equal(t, 3, p.Active())

	fmt.Println("shouldnt be closing here...")

	// Put another resource back, and nothing should expire.
	p.Put(resources[1])
	time.Sleep(timeout * 2)

	expected.InPool = 1
	expected.InUse = 2
	require.Equal(t, expected, p.State())
	require.Equal(t, 3, p.Active())

	// Clean up
	p.Put(resources[2])
	p.Put(resources[3])
}

func TestNewMinActiveSelfRefreshing(t *testing.T) {
	p := NewNewPool(PoolFactory, 5, 5, time.Second, 3)
	defer p.Close()

	// Get 5
	var resources []Resource
	for i := 0; i < 5; i++ {
		resources = append(resources, get(t, p))
	}

	// Put back all nils except one
	for i := 0; i < 4; i++ {
		p.Put(nil)
	}
	p.Put(resources[4])

	require.Equal(t, 3, p.Active())
}

func TestNewMinActiveTooHigh(t *testing.T) {
	defer func() {
		if r := recover(); r.(error).Error() != "minActive 2 higher than capacity 1" {
			t.Errorf("Did not panic correctly: %v", r)
		}
	}()

	p := NewNewPool(FailFactory, 1, 1, time.Second, 2)
	defer p.Close()
}

func TestNewMinActiveTooHighAfterSetCapacity(t *testing.T) {
	p := NewNewPool(FailFactory, 3, 3, time.Second, 2)
	defer p.Close()

	if err := p.SetCapacity(2, true); err != nil {
		t.Errorf("Expecting no error, instead got: %v", err)
	}

	err := p.SetCapacity(1, true)
	expecting := "minActive 2 would now be higher than capacity 1"
	if err == nil || err.Error() != expecting {
		t.Errorf("Expecting: %v, instead got: %v", expecting, err)
	}
}

func TestNewGetPutRace(t *testing.T) {
	p := NewNewPool(SlowCreateFactory, 1, 1, 10*time.Nanosecond, 0)
	defer p.Close()

	for j := 0; j < 200; j++ {
		fmt.Println("\n\n\n------------")
		done := make(chan bool)
		for i := 0; i < 2; i++ {
			go func() {
				time.Sleep(time.Duration(rand.Int() % 10))
				r, err := p.Get(context.Background())
				if err != nil {
					fmt.Println(err)
					panic(err)
				}

				time.Sleep(time.Duration(rand.Int() % 10))

				p.Put(r)
				done <- true
			}()
			runtime.Gosched()
		}
		<-done
		<-done
		fmt.Println("------------DONE")
	}
}

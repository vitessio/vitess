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

package event

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testInterface1 interface {
	TestFunc1()
}

type testInterface2 interface {
	TestFunc2()
}

type testEvent1 struct {
}

type testEvent2 struct {
	triggered bool
}

func (testEvent1) TestFunc1() {}

func (*testEvent2) TestFunc2() {}

func clearListeners() {
	listenersMutex.Lock()
	defer listenersMutex.Unlock()

	listeners = make(map[reflect.Type][]any)
	interfaces = make([]reflect.Type, 0)
}

func TestStaticListener(t *testing.T) {
	clearListeners()

	triggered := false
	AddListener(func(testEvent1) { triggered = true })
	AddListener(func(testEvent2) {
		assert.Fail(t, "wrong listener type triggered")
	})
	Dispatch(testEvent1{})
	assert.True(t, triggered, "static listener failed to trigger")
}

func TestPointerListener(t *testing.T) {
	clearListeners()

	testEvent := new(testEvent2)
	AddListener(func(ev *testEvent2) { ev.triggered = true })
	AddListener(func(testEvent2) {
		assert.Fail(t, "non-pointer listener triggered on pointer type")
	})
	Dispatch(testEvent)
	assert.True(t, testEvent.triggered, "pointer listener failed to trigger")
}

func TestInterfaceListener(t *testing.T) {
	clearListeners()

	triggered := false
	AddListener(func(testInterface1) { triggered = true })
	AddListener(func(testInterface2) {
		assert.Fail(t, "interface listener triggered on non-matching type")
	})
	Dispatch(testEvent1{})
	assert.True(t, triggered, "interface listener failed to trigger")
}

func TestEmptyInterfaceListener(t *testing.T) {
	clearListeners()

	triggered := false
	AddListener(func(any) { triggered = true })
	Dispatch("this should match any")
	assert.True(t, triggered, "empty listener failed to trigger")
}

func TestMultipleListeners(t *testing.T) {
	clearListeners()

	triggered1, triggered2 := false, false
	AddListener(func(testEvent1) { triggered1 = true })
	AddListener(func(testEvent1) { triggered2 = true })
	Dispatch(testEvent1{})

	assert.True(t, triggered1, "listener 1 failed to trigger")
	assert.True(t, triggered2, "listener 2 failed to trigger")
}

func TestBadListenerWrongInputs(t *testing.T) {
	clearListeners()

	defer func() {
		err := recover()
		assert.NotNil(t, err, "bad listener func (wrong # of inputs) failed to trigger panic")
		if err == nil {
			return
		}

		blErr, ok := err.(BadListenerError)
		if !ok {
			panic(err) // this is not the error we were looking for; re-panic
		}

		want := "bad listener func: listener must take exactly one input argument"
		assert.Equal(t, want, blErr.Error())
	}()

	AddListener(func() {})
	Dispatch(testEvent1{})
}

func TestBadListenerWrongType(t *testing.T) {
	clearListeners()

	defer func() {
		err := recover()
		assert.NotNil(t, err, "bad listener type (not a func) failed to trigger panic")

		blErr, ok := err.(BadListenerError)
		if !ok {
			panic(err) // this is not the error we were looking for; re-panic
		}

		want := "bad listener func: listener must be a function"
		assert.Equal(t, want, blErr.Error())
	}()

	AddListener("this is not a function")
	Dispatch(testEvent1{})
}

func TestAsynchronousDispatch(t *testing.T) {
	clearListeners()

	triggered := make(chan bool)
	AddListener(func(testEvent1) { triggered <- true })
	go Dispatch(testEvent1{})

	select {
	case <-triggered:
	case <-time.After(time.Second):
		assert.Fail(t, "asynchronous dispatch failed to trigger listener")
	}
}

func TestDispatchPointerToValueInterfaceListener(t *testing.T) {
	clearListeners()

	triggered := false
	AddListener(func(ev testInterface1) {
		triggered = true
	})
	Dispatch(&testEvent1{})
	assert.True(t, triggered, "Dispatch by pointer failed to trigger interface listener")

}

func TestDispatchValueToValueInterfaceListener(t *testing.T) {
	clearListeners()

	triggered := false
	AddListener(func(ev testInterface1) {
		triggered = true
	})
	Dispatch(testEvent1{})
	assert.True(t, triggered, "Dispatch by value failed to trigger interface listener")
}

func TestDispatchPointerToPointerInterfaceListener(t *testing.T) {
	clearListeners()

	triggered := false
	AddListener(func(testInterface2) { triggered = true })
	Dispatch(&testEvent2{})
	assert.True(t, triggered, "interface listener failed to trigger for pointer")

}

func TestDispatchValueToPointerInterfaceListener(t *testing.T) {
	clearListeners()

	AddListener(func(testInterface2) {
		assert.Fail(t, "interface listener triggered for value dispatch")
	})
	Dispatch(testEvent2{})
}

type testUpdateEvent struct {
	update any
}

func (ev *testUpdateEvent) Update(update any) {
	ev.update = update
}

func TestDispatchUpdate(t *testing.T) {
	clearListeners()

	triggered := false
	AddListener(func(*testUpdateEvent) {
		triggered = true
	})

	ev := &testUpdateEvent{}
	DispatchUpdate(ev, "hello")
	assert.True(t, triggered, "listener failed to trigger on DispatchUpdate()")

	want := "hello"
	got := ev.update.(string)
	assert.Equal(t, want, got)
}

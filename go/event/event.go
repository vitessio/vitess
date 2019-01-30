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

package event

import (
	"fmt"
	"reflect"
	"sync"
)

var (
	listenersMutex sync.RWMutex // protects listeners and interfaces
	listeners      = make(map[reflect.Type][]interface{})
	interfaces     = make([]reflect.Type, 0)
)

// BadListenerError is raised via panic() when AddListener is called with an
// invalid listener function.
type BadListenerError string

func (why BadListenerError) Error() string {
	return fmt.Sprintf("bad listener func: %s", string(why))
}

// AddListener registers a listener function that will be called when a matching
// event is dispatched. The type of the function's first (and only) argument
// declares the event type (or interface) to listen for.
func AddListener(fn interface{}) {
	listenersMutex.Lock()
	defer listenersMutex.Unlock()

	fnType := reflect.TypeOf(fn)

	// check that the function type is what we think: # of inputs/outputs, etc.
	// panic if conditions not met (because it's a programming error to have that happen)
	switch {
	case fnType.Kind() != reflect.Func:
		panic(BadListenerError("listener must be a function"))
	case fnType.NumIn() != 1:
		panic(BadListenerError("listener must take exactly one input argument"))
	}

	// the first input parameter is the event
	evType := fnType.In(0)

	// keep a list of listeners for each event type
	listeners[evType] = append(listeners[evType], fn)

	// if eventType is an interface, store it in a separate list
	// so we can check non-interface objects against all interfaces
	if evType.Kind() == reflect.Interface {
		interfaces = append(interfaces, evType)
	}
}

// Dispatch sends an event to all registered listeners that were declared
// to accept values of the event's type, or interfaces that the value implements.
func Dispatch(ev interface{}) {
	listenersMutex.RLock()
	defer listenersMutex.RUnlock()

	evType := reflect.TypeOf(ev)
	vals := []reflect.Value{reflect.ValueOf(ev)}

	// call listeners for the actual static type
	callListeners(evType, vals)

	// also check if the type implements any of the registered interfaces
	for _, in := range interfaces {
		if evType.Implements(in) {
			callListeners(in, vals)
		}
	}
}

func callListeners(t reflect.Type, vals []reflect.Value) {
	for _, fn := range listeners[t] {
		reflect.ValueOf(fn).Call(vals)
	}
}

// Updater is an interface that events can implement to combine updating and
// dispatching into one call.
type Updater interface {
	// Update is called by DispatchUpdate() before the event is dispatched.
	Update(update interface{})
}

// DispatchUpdate calls Update() on the event and then dispatches it. This is a
// shortcut for combining updates and dispatches into a single call.
func DispatchUpdate(ev Updater, update interface{}) {
	ev.Update(update)
	Dispatch(ev)
}

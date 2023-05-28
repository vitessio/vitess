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

package framework

import (
	"errors"
	"time"

	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func NewQueryCatcher() *EventCatcher[*tabletenv.LogStats] {
	return NewEventCatcher(tabletenv.StatsLogger)
}

type EventCatcher[T Event] struct {
	start   time.Time
	logger  *streamlog.StreamLogger[T]
	in, out chan T
}

type Event interface {
	EventTime() time.Time
}

func NewEventCatcher[T Event](logger *streamlog.StreamLogger[T]) *EventCatcher[T] {
	catcher := &EventCatcher[T]{
		start:  time.Now(),
		logger: logger,
		in:     logger.Subscribe("endtoend"),
		out:    make(chan T, 20),
	}
	go func() {
		for event := range catcher.in {
			endTime := event.EventTime()
			if endTime.Before(catcher.start) {
				continue
			}
			catcher.out <- event
		}
		close(catcher.out)
	}()
	return catcher
}

// Close closes the EventCatcher.
func (catcher *EventCatcher[T]) Close() {
	catcher.logger.Unsubscribe(catcher.in)
	close(catcher.in)
}

func (catcher *EventCatcher[T]) Next() (T, error) {
	tmr := time.NewTimer(5 * time.Second)
	defer tmr.Stop()
	for {
		select {
		case event := <-catcher.out:
			return event, nil
		case <-tmr.C:
			var zero T
			return zero, errors.New("error waiting for query event")
		}
	}
}

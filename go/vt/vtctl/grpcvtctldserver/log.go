/*
Copyright 2021 The Vitess Authors.

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

package grpcvtctldserver

import (
	"sync"

	"vitess.io/vitess/go/vt/logutil"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
)

func eventStreamLogger() (logutil.Logger, func() []*logutilpb.Event) {
	var (
		m      sync.Mutex
		events []*logutilpb.Event
		logger = logutil.NewCallbackLogger(func(e *logutilpb.Event) {
			m.Lock()
			defer m.Unlock()
			events = append(events, e)
		})
	)

	return logger, func() []*logutilpb.Event {
		m.Lock()
		defer m.Unlock()
		return events
	}
}

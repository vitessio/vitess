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

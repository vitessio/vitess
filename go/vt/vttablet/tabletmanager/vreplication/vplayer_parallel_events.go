/*
Copyright 2025 The Vitess Authors.
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

package vreplication

import (
	"math"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

var (
	considerCommitWorkerEvent = &binlogdatapb.VEvent{
		Type:           binlogdatapb.VEventType_UNKNOWN,
		SequenceNumber: math.MinInt64,
	}
)

func (p *parallelProducer) generateConsiderCommitWorkerEvent() *binlogdatapb.VEvent {
	return considerCommitWorkerEvent
}

func (p *parallelProducer) generateCommitWorkerEvent() *binlogdatapb.VEvent {
	return &binlogdatapb.VEvent{
		Type:           binlogdatapb.VEventType_UNKNOWN,
		SequenceNumber: p.commitWorkerEventSequence.Add(-1),
	}
}

func isConsiderCommitWorkerEvent(event *binlogdatapb.VEvent) bool {
	return event.Type == binlogdatapb.VEventType_UNKNOWN && event.SequenceNumber == math.MinInt64
}

func isCommitWorkerEvent(event *binlogdatapb.VEvent) bool {
	return event.Type == binlogdatapb.VEventType_UNKNOWN && event.SequenceNumber < 0
}

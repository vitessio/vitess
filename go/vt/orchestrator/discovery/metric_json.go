/*
   Copyright 2017 Simon J Mudd

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

package discovery

// Collect discovery metrics and manage their storage and retrieval for monitoring purposes.

import (
	"errors"
	"fmt"
	"time"

	"vitess.io/vitess/go/vt/orchestrator/collection"
)

// formattedFloat is to force the JSON output to show 3 decimal places
type formattedFloat float64

func (m formattedFloat) String() string {
	return fmt.Sprintf("%.3f", m)
}

// MetricJSON holds a structure which represents some discovery latency information
type MetricJSON struct {
	Timestamp              time.Time
	Hostname               string
	Port                   int
	BackendLatencySeconds  formattedFloat
	InstanceLatencySeconds formattedFloat
	TotalLatencySeconds    formattedFloat
	Err                    error
}

// JSONSince returns an API response of discovery metric collection information
// in a printable JSON format.
func JSONSince(c *collection.Collection, t time.Time) ([](MetricJSON), error) {
	if c == nil {
		return nil, errors.New("MetricCollection.JSONSince: c == nil")
	}
	raw, err := c.Since(t)
	if err != nil {
		return nil, err
	}

	// build up JSON response for each Metric we received
	var s []MetricJSON
	for i := range raw {
		m := raw[i].(*Metric) // convert back to a real Metric rather than collection.Metric interface
		mj := MetricJSON{
			Timestamp:              m.Timestamp,
			Hostname:               m.InstanceKey.Hostname,
			Port:                   m.InstanceKey.Port,
			BackendLatencySeconds:  formattedFloat(m.BackendLatency.Seconds()),
			InstanceLatencySeconds: formattedFloat(m.InstanceLatency.Seconds()),
			TotalLatencySeconds:    formattedFloat(m.TotalLatency.Seconds()),
			Err:                    m.Err,
		}
		s = append(s, mj)
	}
	return s, nil
}

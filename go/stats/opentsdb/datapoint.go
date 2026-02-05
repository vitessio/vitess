/*
Copyright 2023 The Vitess Authors.

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

package opentsdb

import (
	"fmt"
	"strconv"
	"strings"
)

// DataPoint represents a single OpenTSDB data point.
type DataPoint struct {
	// Example: sys.cpu.nice
	Metric string `json:"metric"`
	// Seconds or milliseconds since unix epoch.
	Timestamp float64           `json:"timestamp"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags"`
}

func (dp *DataPoint) MarshalText() (string, error) {
	var sb strings.Builder

	if _, err := sb.WriteString(fmt.Sprintf("%s %f %f", dp.Metric, dp.Timestamp, dp.Value)); err != nil {
		return "", err
	}

	for k, v := range dp.Tags {
		if _, err := sb.WriteString(fmt.Sprintf(" %s=%s", k, v)); err != nil {
			return "", err
		}
	}

	if _, err := sb.WriteString("\n"); err != nil {
		return "", err
	}

	return sb.String(), nil
}

func unmarshalTextToData(dp *DataPoint, text []byte) error {
	parts := strings.Split(string(text), " ")

	if len(parts) < 3 {
		// Technically every OpenTSDB time series requires at least one tag,
		// but some of the metrics we send have zero.
		return fmt.Errorf("require format: <metric> <timestamp> <value> [<tagk=tagv> <tagkN=tagkV>]")
	}

	dp.Metric = parts[0]

	timestamp, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return err
	}
	dp.Timestamp = timestamp

	value, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return err
	}
	dp.Value = value

	for _, kv := range parts[3:] {
		tagParts := strings.Split(kv, "=")
		if len(tagParts) != 2 {
			return fmt.Errorf("require tag format: <tagk=tagv>")
		}
		if dp.Tags == nil {
			dp.Tags = make(map[string]string)
		}
		dp.Tags[tagParts[0]] = tagParts[1]
	}

	return nil
}

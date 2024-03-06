/*
Copyright 2024 The Vitess Authors.

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
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRead(t *testing.T) {
	invalidInputs := []string{
		"testMetric 0.100000 1.100000 key1=val1 key2=val2",
		"InvalidMarshalText\n",
	}

	for _, in := range invalidInputs {
		mockReader := bytes.NewBufferString(in)
		dpr := NewDataPointReader(mockReader)
		dp, err := dpr.Read()

		assert.Error(t, err)
		assert.Nil(t, dp)
	}

	mockReader := bytes.NewBufferString("testMetric 0.100000 1.100000 key1=val1 key2=val2\n")
	dpr := NewDataPointReader(mockReader)
	dp, err := dpr.Read()

	assert.NoError(t, err)

	expectedDataPoint := DataPoint{
		Metric:    "testMetric",
		Timestamp: 0.1,
		Value:     1.1,
		Tags: map[string]string{
			"key1": "val1",
			"key2": "val2",
		},
	}
	assert.Equal(t, expectedDataPoint, *dp)
}

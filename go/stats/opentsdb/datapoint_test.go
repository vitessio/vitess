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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshalText(t *testing.T) {
	dp := DataPoint{
		Metric:    "testMetric",
		Timestamp: 0.1,
		Value:     1.1,
		Tags: map[string]string{
			"key1": "val1",
		},
	}

	str, err := dp.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "testMetric 0.100000 1.100000 key1=val1\n", str)
}

func TestUnmarshalTextToData(t *testing.T) {
	dp := DataPoint{}

	invalidMarshalTestCases := []string{
		"InvalidMarshalText",
		"testMetric invalidFloat invalidFloat",
		"testMetric 0.100000 invalidFloat",
		"testMetric 0.100000 1.100000 invalidKey:ValuePair",
	}

	for _, text := range invalidMarshalTestCases {
		err := unmarshalTextToData(&dp, []byte(text))
		assert.Error(t, err)
	}

	err := unmarshalTextToData(&dp, []byte("testMetric 0.100000 1.100000 key1=val1 key2=val2"))
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
	assert.Equal(t, expectedDataPoint, dp)
}

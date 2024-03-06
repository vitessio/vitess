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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWrite(t *testing.T) {
	sampleData := []*DataPoint{
		{
			Metric:    "testMetric",
			Timestamp: 1.0,
			Value:     2.0,
			Tags: map[string]string{
				"tag1": "value1",
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var receivedData []*DataPoint
		err := json.NewDecoder(r.Body).Decode(&receivedData)

		assert.NoError(t, err)
		assert.Len(t, receivedData, 1)
		assert.Equal(t, sampleData[0], receivedData[0])

		w.WriteHeader(http.StatusOK)
	}))

	defer server.Close()

	client := &http.Client{}
	hw := newHTTPWriter(client, server.URL)

	err := hw.Write(sampleData)
	assert.NoError(t, err)
}

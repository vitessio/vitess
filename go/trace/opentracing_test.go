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

package trace

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
)

func TestExtractMapFromString(t *testing.T) {
	expected := make(opentracing.TextMapCarrier)
	expected["uber-trace-id"] = "123:456:789:1"
	expected["other data with weird symbols:!#;"] = ":1!\""
	jsonBytes, err := json.Marshal(expected)
	assert.NoError(t, err)

	encodedString := base64.StdEncoding.EncodeToString(jsonBytes)

	result, err := extractMapFromString(encodedString)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestErrorConditions(t *testing.T) {
	encodedString := base64.StdEncoding.EncodeToString([]byte(`{"key":42}`))
	_, err := extractMapFromString(encodedString) // malformed json {"key":42}
	assert.Error(t, err)

	_, err = extractMapFromString("this is not base64") // malformed base64
	assert.Error(t, err)
}

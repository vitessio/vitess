/*
Copyright 2019 Google Inc.

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
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
)

func TestExtractMapFromString(t *testing.T) {
	expected := make(opentracing.TextMapCarrier)
	expected["apa"] = "12"
	expected["banan"] = "x-tracing-backend-12"
	result, err := extractMapFromString("apa=12:banan=x-tracing-backend-12")
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestErrorConditions(t *testing.T) {
	_, err := extractMapFromString("")
	assert.Error(t, err)

	_, err = extractMapFromString("key=value:keywithnovalue")
	assert.Error(t, err)
}

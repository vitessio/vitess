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

package trace

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestExtractCarrierFromString(t *testing.T) {
	expected := map[string]string{
		"traceparent":                       "00-12345678901234567890123456789012-1234567890123456-01",
		"other data with weird symbols:!#;": ":1!\"",
	}
	jsonBytes, err := json.Marshal(expected)
	assert.NoError(t, err)

	encodedString := base64.StdEncoding.EncodeToString(jsonBytes)

	result, err := extractCarrierFromString(encodedString)
	assert.NoError(t, err)
	assert.Equal(t, propagation.MapCarrier(expected), result)
}

func TestExtractCarrierFromStringErrorConditions(t *testing.T) {
	encodedString := base64.StdEncoding.EncodeToString([]byte(`{"key":42}`))
	_, err := extractCarrierFromString(encodedString) // malformed json: value is not a string
	assert.Error(t, err)

	_, err = extractCarrierFromString("this is not base64") // malformed base64
	assert.Error(t, err)
}

func TestOtelSpan(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() {
		require.NoError(t, tp.Shutdown(t.Context()))
	}()

	tracer := tp.Tracer("test")
	svc := otelTracingService{Tracer: tracer}

	clientSpan, ctx := svc.New(t.Context(), "test-label")
	require.NotEmpty(t, clientSpan)
	require.NotNil(t, ctx)

	clientSpan, ctx = svc.New(ctx, "client-span")
	require.NotEmpty(t, clientSpan)
	require.NotNil(t, ctx)

	spanFromCtx, ok := svc.FromContext(context.Background())
	require.False(t, ok)
	require.Nil(t, spanFromCtx)

	ctx = svc.NewContext(t.Context(), clientSpan)
	require.NotNil(t, ctx)
	clientSpan.Finish()

	spanFromCtx, ok = svc.FromContext(ctx)
	require.True(t, ok)
	require.NotEmpty(t, spanFromCtx)

	// NewContext with non-otelSpan should return parent context unchanged.
	ctx = svc.NewContext(context.TODO(), &mockSpan{})
	require.Equal(t, context.TODO(), ctx)
}

func TestKeyValue(t *testing.T) {
	kv := keyValue("str", "value")
	assert.Equal(t, "str", string(kv.Key))

	kv = keyValue("bool", true)
	assert.Equal(t, "bool", string(kv.Key))

	kv = keyValue("int", 42)
	assert.Equal(t, "int", string(kv.Key))

	kv = keyValue("int64", int64(42))
	assert.Equal(t, "int64", string(kv.Key))

	kv = keyValue("float64", 3.14)
	assert.Equal(t, "float64", string(kv.Key))

	kv = keyValue("other", []int{1, 2, 3})
	assert.Equal(t, "other", string(kv.Key))
}

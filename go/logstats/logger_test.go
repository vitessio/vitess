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

package logstats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestInit(t *testing.T) {
	tl := Logger{}

	tl.Init(false)
	assert.Nil(t, tl.b)
	assert.Equal(t, 0, tl.n)
	assert.Equal(t, false, tl.json)

	tl.Init(true)
	assert.Equal(t, []byte{'{'}, tl.b)
	assert.Equal(t, 0, tl.n)
	assert.Equal(t, true, tl.json)
}

func TestRedacted(t *testing.T) {
	tl := Logger{}
	tl.Init(false)

	tl.Redacted()
	assert.Equal(t, []byte("\"[REDACTED]\""), tl.b)

	// Test for json
	tl.b = []byte{}
	tl.Init(true)

	tl.Redacted()
	assert.Equal(t, []byte("{\"[REDACTED]\""), tl.b)
}

func TestKey(t *testing.T) {
	tl := Logger{
		b: []byte("test"),
	}
	tl.Init(false)

	// Expect tab not be appended at first
	tl.Key("testKey")
	assert.Equal(t, []byte("test"), tl.b)

	tl.Key("testKey")
	assert.Equal(t, []byte("test\t"), tl.b)

	tl.b = []byte{}
	tl.Init(true)

	tl.Key("testKey")
	assert.Equal(t, []byte("{\"testKey\": "), tl.b)

	tl.Key("testKey2")
	assert.Equal(t, []byte("{\"testKey\": , \"testKey2\": "), tl.b)
}

func TestStringUnquoted(t *testing.T) {
	tl := Logger{}
	tl.Init(true)

	tl.StringUnquoted("testValue")
	assert.Equal(t, []byte("{\"testValue\""), tl.b)

	tl.b = []byte{}
	tl.Init(false)

	tl.StringUnquoted("testValue")
	assert.Equal(t, []byte("testValue"), tl.b)
}

func TestTabTerminated(t *testing.T) {
	tl := Logger{}
	tl.Init(true)

	tl.TabTerminated()
	// Should not be tab terminated in case of json
	assert.Equal(t, []byte("{"), tl.b)

	tl.b = []byte("test")
	tl.Init(false)

	tl.TabTerminated()
	assert.Equal(t, []byte("test\t"), tl.b)
}

func TestString(t *testing.T) {
	tl := Logger{}
	tl.Init(true)

	tl.String("testValue")
	assert.Equal(t, []byte("{\"testValue\""), tl.b)

	tl.b = []byte{}
	tl.Init(false)

	tl.String("testValue")
	assert.Equal(t, []byte("\"testValue\""), tl.b)
}

func TestStringSingleQuoted(t *testing.T) {
	tl := Logger{}
	tl.Init(true)

	tl.StringSingleQuoted("testValue")
	// Should be double quoted in case of json
	assert.Equal(t, []byte("{\"testValue\""), tl.b)

	tl.b = []byte{}
	tl.Init(false)

	tl.StringSingleQuoted("testValue")
	assert.Equal(t, []byte("'testValue'"), tl.b)
}

func TestTime(t *testing.T) {
	tl := Logger{}
	tl.Init(false)

	testTime := time.Date(2024, 9, 3, 7, 10, 12, 1233, time.UTC)
	tl.Time(testTime)
	assert.Equal(t, []byte("2024-09-03 07:10:12.000001"), tl.b)

	tl.b = []byte{}
	tl.Init(true)

	tl.Time(testTime)
	assert.Equal(t, []byte("{\"2024-09-03 07:10:12.000001\""), tl.b)
}

func TestDuration(t *testing.T) {
	tl := Logger{}
	tl.Init(false)

	tl.Duration(2 * time.Minute)
	assert.Equal(t, []byte("120.000000"), tl.b)

	tl.b = []byte{}
	tl.Init(true)

	tl.Duration(6 * time.Microsecond)
	assert.Equal(t, []byte("{0.000006"), tl.b)
}

func TestInt(t *testing.T) {
	tl := Logger{}
	tl.Init(false)

	tl.Int(98)
	assert.Equal(t, []byte("98"), tl.b)

	tl.b = []byte{}
	tl.Init(true)

	tl.Int(-1234)
	assert.Equal(t, []byte("{-1234"), tl.b)
}

func TestUint(t *testing.T) {
	tl := Logger{}
	tl.Init(false)

	tl.Uint(98)
	assert.Equal(t, []byte("98"), tl.b)

	tl.b = []byte{}
	tl.Init(true)

	tl.Uint(1234)
	assert.Equal(t, []byte("{1234"), tl.b)
}

func TestBool(t *testing.T) {
	tl := Logger{}
	tl.Init(false)

	tl.Bool(true)
	assert.Equal(t, []byte("true"), tl.b)

	tl.b = []byte{}
	tl.Init(true)

	tl.Bool(false)
	assert.Equal(t, []byte("{false"), tl.b)
}

func TestStrings(t *testing.T) {
	tl := Logger{}
	tl.Init(false)

	tl.Strings([]string{"testValue1", "testValue2"})
	assert.Equal(t, []byte("[\"testValue1\",\"testValue2\"]"), tl.b)

	tl.b = []byte{}
	tl.Init(true)

	tl.Strings([]string{"testValue1"})
	assert.Equal(t, []byte("{[\"testValue1\"]"), tl.b)
}

var calledValue []byte

type mockWriter struct{}

func (*mockWriter) Write(p []byte) (int, error) {
	calledValue = p
	return 1, nil
}

func TestFlush(t *testing.T) {
	tl := NewLogger()
	tl.Init(true)

	tl.Key("testKey")
	tl.String("testValue")

	tw := mockWriter{}

	err := tl.Flush(&tw)
	assert.NoError(t, err)
	assert.Equal(t, []byte("{\"testKey\": \"testValue\"}\n"), calledValue)
}

func TestBindVariables(t *testing.T) {
	tcases := []struct {
		name  string
		bVars map[string]*querypb.BindVariable
		want  []byte
		full  bool
	}{
		{
			name: "int32, float64",
			bVars: map[string]*querypb.BindVariable{
				"v1": sqltypes.Int32BindVariable(10),
				"v2": sqltypes.Float64BindVariable(10.122),
			},
			want: []byte(`{{"v1": {"type": "INT32", "value": 10}, "v2": {"type": "FLOAT64", "value": 10.122}}`),
		},
		{
			name: "varbinary, float64",
			bVars: map[string]*querypb.BindVariable{
				"v1": {
					Type:  querypb.Type_VARBINARY,
					Value: []byte("aa"),
				},
				"v2": sqltypes.Float64BindVariable(10.122),
			},
			want: []byte(`{{"v1": {"type": "VARBINARY", "value": "0 bytes"}, "v2": {"type": "FLOAT64", "value": 10.122}}`),
		},
		{
			name: "varbinary, varchar",
			bVars: map[string]*querypb.BindVariable{
				"v1": {
					Type:  querypb.Type_VARBINARY,
					Value: []byte("abc"),
				},
				"v2": {
					Type:  querypb.Type_VARCHAR,
					Value: []byte("aa"),
				},
			},
			full: true,
			want: []byte(`{{"v1": {"type": "VARBINARY", "value": "abc"}, "v2": {"type": "VARCHAR", "value": "aa"}}`),
		},

		{
			name: "int64, tuple",
			bVars: map[string]*querypb.BindVariable{
				"v1": {
					Type:  querypb.Type_INT64,
					Value: []byte("12"),
				},
				"v2": {
					Type: querypb.Type_TUPLE,
					Values: []*querypb.Value{{
						Type:  querypb.Type_VARCHAR,
						Value: []byte("aa"),
					}, {
						Type:  querypb.Type_VARCHAR,
						Value: []byte("bb"),
					}},
				},
			},
			want: []byte(`{{"v1": {"type": "INT64", "value": 12}, "v2": {"type": "TUPLE", "value": "2 items"}}`),
		},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			tl := Logger{}
			tl.Init(true)

			tl.BindVariables(tc.bVars, tc.full)
			assert.Equal(t, tc.want, tl.b)
		})
	}
}

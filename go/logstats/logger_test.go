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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestInit(t *testing.T) {
	tl := Logger{}

	tl.Init(false)
	assert.Nil(t, tl.b)
	assert.Equal(t, 0, tl.n)
	assert.False(t, tl.json)

	tl.Init(true)
	assert.Equal(t, []byte{'{'}, tl.b)
	assert.Equal(t, 0, tl.n)
	assert.True(t, tl.json)
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
	require.NoError(t, err)
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
			want: []byte(`{{"v1": {"type": "VARBINARY", "value": "2 bytes"}, "v2": {"type": "FLOAT64", "value": 10.122}}`),
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

var logNumberPattern = regexp.MustCompile(`^([+-]?)([0-9]*)(\.[0-9]*)?([eE][+-]?[0-9]+)?$`)

func logNumberReference(typ querypb.Type, value []byte) (string, bool) {
	parts := logNumberPattern.FindStringSubmatch(strings.Trim(string(value), " \t\r\n"))
	if parts == nil || parts[2] == "" && len(parts[3]) <= 1 {
		return "", false
	}
	if sqltypes.IsIntegral(typ) && (parts[3] != "" || parts[4] != "" || sqltypes.IsUnsigned(typ) && parts[1] == "-") {
		return "", false
	}
	sign := parts[1]
	if sign == "+" {
		sign = ""
	}
	integer := strings.TrimLeft(parts[2], "0")
	if integer == "" {
		integer = "0"
	}
	fraction := parts[3]
	if fraction == "." {
		fraction = ".0"
	}
	return sign + integer + fraction + parts[4], true
}

func checkNumericBindVariable(t *testing.T, typ querypb.Type, value []byte) {
	t.Helper()
	number, isNumber := logNumberReference(typ, value)

	bindVars := map[string]*querypb.BindVariable{"v": {Type: typ, Value: value}}
	for _, jsonFormat := range []bool{false, true} {
		for _, full := range []bool{false, true} {
			log := NewLogger()
			log.Init(jsonFormat)
			log.Key("BindVars")
			log.BindVariables(bindVars, full)
			var out bytes.Buffer
			require.NoError(t, log.Flush(&out))
			assert.Equal(t, 1, bytes.Count(out.Bytes(), []byte{'\n'}))
			assert.NotContains(t, out.String(), "\t")
			assert.NotContains(t, out.String(), "\r")
			encoded := out.Bytes()
			if jsonFormat {
				var record struct{ BindVars json.RawMessage }
				require.NoError(t, json.Unmarshal(encoded, &record))
				encoded = record.BindVars
			}
			var bindings map[string]struct {
				Type  string
				Value json.RawMessage
			}
			require.NoError(t, json.Unmarshal(encoded, &bindings))
			require.Len(t, bindings, 1)
			binding, ok := bindings["v"]
			require.True(t, ok)
			assert.Equal(t, typ.String(), binding.Type)
			if isNumber {
				assert.Equal(t, number, string(binding.Value))
			} else {
				var text string
				require.NoError(t, json.Unmarshal(binding.Value, &text))
				assert.Equal(t, string([]rune(string(value))), text)
			}
		}
	}
}

func TestBindVariablesNumbers(t *testing.T) {
	for _, tc := range []struct {
		typ   querypb.Type
		value string
	}{
		{querypb.Type_INT64, "0"},
		{querypb.Type_INT64, "-9223372036854775808"},
		{querypb.Type_INT64, "9223372036854775807"},
		{querypb.Type_UINT64, "18446744073709551615"},
		{querypb.Type_INT64, "007"},
		{querypb.Type_INT64, "+42"},
		{querypb.Type_INT64, "-01"},
		{querypb.Type_INT64, "000"},
		{querypb.Type_INT64, "-000"},
		{querypb.Type_INT64, "1.5"},
		{querypb.Type_INT64, "1e2"},
		{querypb.Type_UINT64, "-1"},
		{querypb.Type_UINT64, "+0018446744073709551615"},
		{querypb.Type_INT64, " 42 "},
		{querypb.Type_INT64, "\t42"},
		{querypb.Type_INT64, "42\t"},
		{querypb.Type_INT64, "\t42\t"},
		{querypb.Type_INT64, "4\t2"},
		{querypb.Type_INT64, `\t42\t`},
		{querypb.Type_INT64, "42\r"},
		{querypb.Type_UINT64, "\t42\t"},
		{querypb.Type_FLOAT64, "\t4.2\t"},
		{querypb.Type_INT64, " \t42\r\n"},
		{querypb.Type_INT64, "\u00a042"},
		{querypb.Type_FLOAT64, "-0"},
		{querypb.Type_FLOAT64, "1.2345678901234567890"},
		{querypb.Type_FLOAT64, "1.25E+002"},
		{querypb.Type_FLOAT64, "1e9999"},
		{querypb.Type_FLOAT64, "NaN"},
		{querypb.Type_FLOAT64, "Inf"},
		{querypb.Type_FLOAT64, "-Infinity"},
		{querypb.Type_FLOAT64, "+5"},
		{querypb.Type_FLOAT64, ".5"},
		{querypb.Type_FLOAT64, "1."},
		{querypb.Type_FLOAT64, "1.e2"},
		{querypb.Type_FLOAT64, "0.e2"},
		{querypb.Type_FLOAT64, "000e2"},
		{querypb.Type_FLOAT64, " \t+.12345678901234567890e+002\t "},
		{querypb.Type_FLOAT64, "-001.2345678901234567890"},
		{querypb.Type_FLOAT64, "."},
		{querypb.Type_FLOAT64, ".e2"},
		{querypb.Type_FLOAT64, "+-1"},
		{querypb.Type_FLOAT64, "--1"},
		{querypb.Type_FLOAT64, "1_000"},
		{querypb.Type_FLOAT64, "0x1p2"},
		{querypb.Type_FLOAT64, "1.2.3"},
		{querypb.Type_FLOAT64, "1e+"},
		{querypb.Type_FLOAT64, "1e"},
		{querypb.Type_FLOAT64, ""},
		{querypb.Type_FLOAT64, "null"},
		{querypb.Type_FLOAT64, "true"},
		{querypb.Type_FLOAT64, "{}"},
		{querypb.Type_FLOAT64, "[]"},
		{querypb.Type_FLOAT64, `"1"`},
		{querypb.Type_FLOAT64, "1\n2"},
		{querypb.Type_FLOAT64, "\xff\"\n"},
	} {
		t.Run(fmt.Sprintf("%s/%q", tc.typ, tc.value), func(t *testing.T) {
			checkNumericBindVariable(t, tc.typ, []byte(tc.value))
		})
	}
}

func FuzzBindVariablesNumbers(f *testing.F) {
	for _, value := range []string{"", "42", "NaN", "007", "-0", ".5", "1e+20", "1e9999", "1.e2", ".e2", "0.e2", "+-1", ".12345678901234567890", " 42 ", "\t42", "42\t", "\t42\t", "4\t2", `\t42\t`, "42\r", " \t42\r\n", "1\n2", "\xff\"\n", "null", "[1]"} {
		f.Add([]byte(value))
	}
	f.Fuzz(func(t *testing.T, value []byte) {
		for _, typ := range []querypb.Type{querypb.Type_INT64, querypb.Type_UINT64, querypb.Type_FLOAT64} {
			checkNumericBindVariable(t, typ, value)
		}
	})
}

func jsonQuoteReference(t *testing.T, s string) []byte {
	t.Helper()
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	// Make replacement runes explicit so both JSON backends quote them the same way.
	require.NoError(t, encoder.Encode(string([]rune(s))))
	return bytes.TrimSuffix(buf.Bytes(), []byte{'\n'})
}

func checkQuote(t *testing.T, s string) {
	t.Helper()
	var outputs []struct{ got, want []byte }
	for _, jsonFormat := range []bool{false, true} {
		var quoted []byte
		if jsonFormat {
			quoted = jsonQuoteReference(t, s)
		} else {
			quoted = strconv.AppendQuote(nil, s)
		}
		for _, prefix := range []string{"", "prefix:\x00\xff\u2028\u2029"} {
			want := append([]byte(prefix), quoted...)
			for _, spare := range []int{0, len(s) + 2, 6*len(s) + 2} {
				dst := make([]byte, len(prefix), len(prefix)+spare)
				copy(dst, prefix)
				got := appendQuote(dst, s, jsonFormat)
				require.Equal(t, want, got, "input %q, JSON %v, spare %d", s, jsonFormat, spare)
				outputs = append(outputs, struct{ got, want []byte }{got, want})
			}
		}
	}
	// Reusing the encoder must not change previously returned buffers.
	for _, output := range outputs {
		require.Equal(t, output.want, output.got)
	}
}

func TestAppendQuote(t *testing.T) {
	for _, s := range []string{
		"", "'", "<>&", "\"\\\a\b\f\n\r\t\v\x00\x1f\x7f", "世界 café",
		"\u0085\u00a0\u2028\u2029\ufeff\U0001f600\U000e0001",
		"\u2028", "\u2029", "\\u2028\\u2029", "\u2028\\u2028\u2029\\u2029\xff",
		"\xff\xfe\xed\xa0\x80\xc0\xaf\xe2\x82", "\ufffd",
	} {
		checkQuote(t, s)
		checkQuote(t, strings.Repeat("a", 32)+s+"suffix")
		checkQuote(t, strings.Repeat("\x00", 32)+s+"suffix")
	}
	for _, size := range []int{1, 16, 31, 32, 33, 63, 64, 65, 256} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			t.Parallel()
			checkQuote(t, strings.Repeat("a", size))
			for _, pos := range []int{0, size / 2, size - 1} {
				for c := range 256 {
					s := bytes.Repeat([]byte{'a'}, size)
					s[pos] = byte(c)
					checkQuote(t, string(s))
				}
			}
		})
	}
	for _, pos := range []int{1, 15, 31, 32, 33, 63, 64, 65} {
		for _, suffix := range []string{"世界", "\u2028", "\U000e0001", "\xff\"\n\\"} {
			checkQuote(t, strings.Repeat("a", pos)+suffix+"end")
		}
	}
	rng := rand.New(rand.NewPCG(56, 78))
	for range 1000 {
		raw := make([]byte, rng.IntN(512))
		for i := range raw {
			raw[i] = byte(rng.Uint32())
		}
		checkQuote(t, strings.Repeat("a", rng.IntN(80))+string(raw))
	}
}

func FuzzAppendQuote(f *testing.F) {
	for _, s := range []string{"", strings.Repeat("a", 32), strings.Repeat("a", 64) + "\xff\n\"\\", strings.Repeat("\x00", 32) + "\u2028", "世界 café", "\a\v\x7f\U000e0001", "\u2028\\u2028\u2029\\u2029\xff"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		checkQuote(t, s)
	})
}

func TestLoggerQuoteFormats(t *testing.T) {
	value := strings.Repeat("a", 64) + "\"\\\n\x00\a\v\x7f\u2028\U000e0001\xff"
	bvars := map[string]*querypb.BindVariable{value: sqltypes.StringBindVariable(value)}
	var out bytes.Buffer
	for range 3 {
		for _, jsonFormat := range []bool{false, true} {
			quoted := strconv.Quote(value)
			if jsonFormat {
				quoted = string(jsonQuoteReference(t, value))
			}
			log := NewLogger()
			log.Init(jsonFormat)
			log.Key("string")
			log.String(value)
			log.Key("unquoted")
			log.StringUnquoted(value)
			log.Key("single")
			log.StringSingleQuoted(value)
			log.Key("strings")
			log.Strings([]string{value})
			log.Key("full")
			log.BindVariables(bvars, true)
			log.Key("abbreviated")
			log.BindVariables(bvars, false)
			log.Key("redacted")
			log.Redacted()

			full := "{" + quoted + `: {"type": "VARCHAR", "value": ` + quoted + "}}"
			abbreviated := "{" + quoted + `: {"type": "VARCHAR", "value": "` + strconv.Itoa(len(value)) + ` bytes"}}`
			var want string
			if jsonFormat {
				want = `{"string": ` + quoted + `, "unquoted": ` + quoted + `, "single": ` + quoted + `, "strings": [` + quoted + `], "full": ` + full + `, "abbreviated": ` + abbreviated + `, "redacted": "[REDACTED]"}` + "\n"
			} else {
				want = strings.Join([]string{quoted, value, "'" + value + "'", "[" + quoted + "]", full, abbreviated, `"[REDACTED]"`}, "\t") + "\n"
			}
			out.Reset()
			require.NoError(t, log.Flush(&out))
			assert.Equal(t, want, out.String())
			if jsonFormat {
				var record map[string]any
				require.NoError(t, json.Unmarshal(out.Bytes(), &record))
			}
		}
	}
}

func BenchmarkNumericBindVariables(b *testing.B) {
	for _, format := range []string{"text", "json"} {
		for _, tc := range []struct {
			name  string
			typ   querypb.Type
			value string
		}{
			{"integer", querypb.Type_INT64, "42"},
			{"int64-min", querypb.Type_INT64, "-9223372036854775808"},
			{"uint64-max", querypb.Type_UINT64, "18446744073709551615"},
			{"float", querypb.Type_FLOAT64, "123.456"},
			{"exponent", querypb.Type_FLOAT64, "1.234e+50"},
			{"nan", querypb.Type_FLOAT64, "NaN"},
			{"infinity", querypb.Type_FLOAT64, "Inf"},
			{"leading-zero", querypb.Type_INT64, "007"},
			{"leading-plus", querypb.Type_FLOAT64, "+5"},
			{"fraction", querypb.Type_FLOAT64, ".5"},
			{"trailing-dot", querypb.Type_FLOAT64, "1."},
			{"whitespace", querypb.Type_INT64, " \t42\t "},
			{"float-whitespace", querypb.Type_FLOAT64, " \t123.456\t "},
			{"float-leading-zero", querypb.Type_FLOAT64, "00123.456"},
			{"precise-float", querypb.Type_FLOAT64, "0.12345678901234567890"},
			{"precise-fraction", querypb.Type_FLOAT64, ".12345678901234567890"},
		} {
			b.Run(format+"/"+tc.name, func(b *testing.B) {
				bindVars := map[string]*querypb.BindVariable{"v": {Type: tc.typ, Value: []byte(tc.value)}}
				require.NoError(b, sqltypes.ValidateBindVariables(bindVars))
				emit := func(w io.Writer) error {
					log := NewLogger()
					log.Init(format == "json")
					log.Key("BindVars")
					log.BindVariables(bindVars, true)
					return log.Flush(w)
				}
				var out bytes.Buffer
				require.NoError(b, emit(&out))
				require.NotEmpty(b, out.Bytes())
				b.ReportAllocs()
				b.SetBytes(int64(len(tc.value)))
				for b.Loop() {
					if err := emit(io.Discard); err != nil {
						require.NoError(b, err)
					}
				}
			})
		}
	}
}

type quoteBenchmarkCase struct {
	name string
	text string
}

func quoteBenchmarkCases(b *testing.B) []quoteBenchmarkCase {
	b.Helper()
	var cases []quoteBenchmarkCase
	for _, size := range []int{16, 32, 64, 4096, 65536} {
		for _, shape := range []string{"clean", "sparse", "dense", "unicode", "late-unicode", "js-separators", "late-js-separator", "invalid-utf8"} {
			var text string
			switch shape {
			case "clean":
				text = strings.Repeat("a", size)
			case "sparse":
				payload := []byte(strings.Repeat("a", size))
				for i := min(size/2, 125); i < size; i += 128 {
					copy(payload[i:], "\"\\\n")
				}
				text = string(payload)
				require.Contains(b, text, "\"")
			case "dense":
				text = strings.Repeat("abc\"\\\n", size/6+1)[:size]
			case "unicode":
				text = strings.Repeat("\U0001d11e", size/4)
			case "late-unicode":
				text = strings.Repeat("a", size-4) + "\U0001d11e"
			case "js-separators":
				text = strings.Repeat("\u2028\u2029", size/6) + strings.Repeat("a", size%6)
			case "late-js-separator":
				text = strings.Repeat("a", size-3) + "\u2028"
			case "invalid-utf8":
				text = strings.Repeat("a", size-1) + "\xff"
			}
			require.Len(b, text, size)
			require.Equal(b, shape != "invalid-utf8", utf8.ValidString(text))
			cases = append(cases, quoteBenchmarkCase{fmt.Sprintf("%d/%s", size, shape), text})
		}
	}
	for _, size := range []int{64, 4096} {
		for _, position := range []struct {
			name   string
			offset int
		}{
			{"early", 0},
			{"middle", size / 2},
			{"late", size - 1},
		} {
			payload := []byte(strings.Repeat("a", size))
			payload[position.offset] = '"'
			cases = append(cases, quoteBenchmarkCase{
				fmt.Sprintf("escape-position/%d/%s", size, position.name), string(payload),
			})
		}
	}
	return cases
}

func BenchmarkQuote(b *testing.B) {
	for _, tc := range quoteBenchmarkCases(b) {
		b.Run(tc.name, func(b *testing.B) {
			var log Logger
			log.String(tc.text)
			require.NotEmpty(b, log.b)
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.text)))
			for b.Loop() {
				log.b = log.b[:0]
				log.String(tc.text)
			}
		})
	}
}

func BenchmarkJSONQuote(b *testing.B) {
	for _, tc := range quoteBenchmarkCases(b) {
		b.Run(tc.name, func(b *testing.B) {
			log := Logger{json: true}
			log.String(tc.text)
			require.NotEmpty(b, log.b)
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.text)))
			for b.Loop() {
				log.b = log.b[:0]
				log.String(tc.text)
			}
		})
	}
}

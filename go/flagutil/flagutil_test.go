/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package flagutil

import (
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

func TestStringList(t *testing.T) {
	p := StringListValue([]string{})
	var _ pflag.Value = &p
	wanted := map[string]string{
		"0ala,ma,kota":   "0ala.ma.kota",
		`1ala\,ma,kota`:  "1ala,ma.kota",
		`2ala\\,ma,kota`: `2ala\.ma.kota`,
		"3ala,":          "3ala.",
	}
	for in, out := range wanted {
		if assert.Nil(t, p.Set(in)) {
			continue
		}
		assert.Equal(t, strings.Join(p, "."), out)
		assert.Equal(t, in, p.String())
	}
}

// TestEmptyStringList verifies that an empty parameter results in an empty list
func TestEmptyStringList(t *testing.T) {
	var p StringListValue
	var _ pflag.Value = &p
	if err := p.Set(""); err != nil {
		t.Fatalf("p.Set(\"\"): %v", err)
	}
	if len(p) != 0 {
		t.Fatalf("len(p) != 0: got %v", len(p))
	}
}

type pair struct {
	in  string
	out map[string]string
	err error
}

func TestStringMap(t *testing.T) {
	v := StringMapValue(nil)
	var _ pflag.Value = &v
	wanted := []pair{
		{
			in:  "tag1:value1,tag2:value2",
			out: map[string]string{"tag1": "value1", "tag2": "value2"},
		},
		{
			in:  `tag1:1:value1\,,tag2:value2`,
			out: map[string]string{"tag1": "1:value1,", "tag2": "value2"},
		},
		{
			in:  `tag1:1:value1\,,tag2`,
			err: errInvalidKeyValuePair,
		},
	}
	for _, want := range wanted {
		assert.Equal(t, want.err, v.Set(want.in))
		if want.err != nil {
			continue
		}

		if len(want.out) != len(v) {
			assert.Equal(t, want.out, v)
			continue
		}
		for key, value := range want.out {
			assert.Equal(t, value, v[key])
		}
		assert.Equal(t, want.in, v.String())
	}
}

type lowHighFloat64Values struct {
	in  string
	out *LowHighFloat64Values
	err error
}

func TestLowHighFloat64Values(t *testing.T) {
	v := LowHighFloat64Values{}
	var _ pflag.Value = &v
	wanted := []lowHighFloat64Values{
		{
			in:  "",
			err: errInvalidLowHighFloat64ValuesPair,
		},
		{
			in:  "should:fail",
			err: errInvalidLowHighFloat64ValuesPair,
		},
		{
			in:  "2:1",
			err: errInvalidLowHighFloat64ValuesPair,
		},
		{
			in:  "-1:10",
			err: errInvalidLowHighFloat64ValuesPair,
		},
		{
			in:  "1:101",
			err: errInvalidLowHighFloat64ValuesPair,
		},
		{
			in:  "1:2:3",
			err: errInvalidLowHighFloat64ValuesPair,
		},
		{
			in:  "75.123:90.456",
			out: &LowHighFloat64Values{Low: 75.123, High: 90.456},
		},
		{
			in:  "2",
			out: &LowHighFloat64Values{Low: 2, High: 0},
		},
	}
	for _, want := range wanted {
		assert.ErrorIs(t, want.err, v.Set(want.in))
		if want.out != nil {
			assert.Equal(t, *want.out, v.Get())
		}
	}
}

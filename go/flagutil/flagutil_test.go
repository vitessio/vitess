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

package flagutil

import (
	"flag"
	"strings"
	"testing"
)

func TestStringList(t *testing.T) {
	p := StringListValue([]string{})
	var _ flag.Value = &p
	wanted := map[string]string{
		"0ala,ma,kota":   "0ala.ma.kota",
		`1ala\,ma,kota`:  "1ala,ma.kota",
		`2ala\\,ma,kota`: `2ala\.ma.kota`,
		"3ala,":          "3ala.",
	}
	for in, out := range wanted {
		if err := p.Set(in); err != nil {
			t.Errorf("v.Set(%v): %v", in, err)
			continue
		}
		if strings.Join(p, ".") != out {
			t.Errorf("want %#v, got %#v", strings.Split(out, "."), p)
		}
		if p.String() != in {
			t.Errorf("v.String(): want %#v, got %#v", in, p.String())
		}
	}
}

// TestEmptyStringList verifies that an empty parameter results in an empty list
func TestEmptyStringList(t *testing.T) {
	var p StringListValue
	var _ flag.Value = &p
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
	var _ flag.Value = &v
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
		if err := v.Set(want.in); err != want.err {
			t.Errorf("v.Set(%v): %v", want.in, want.err)
			continue
		}
		if want.err != nil {
			continue
		}

		if len(want.out) != len(v) {
			t.Errorf("want %#v, got %#v", want.out, v)
			continue
		}
		for key, value := range want.out {
			if v[key] != value {
				t.Errorf("want %#v, got %#v", want.out, v)
				continue
			}
		}

		if vs := v.String(); vs != want.in {
			t.Errorf("v.String(): want %#v, got %#v", want.in, vs)
		}
	}
}

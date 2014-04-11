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

type pair struct {
	in  string
	out map[string]string
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
	}
	for _, want := range wanted {
		if err := v.Set(want.in); err != nil {
			t.Errorf("v.Set(%v): %v", want.in, err)
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
		vs := v.String()
		if vs != want.in {
			t.Errorf("v.String(): want %#v, got %#v", want.in, vs)
		}
	}
}

/*
Copyright 2017 Google Inc.

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

package vindexes

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
)

var charVindex Vindex

func init() {
	charVindex, _ = CreateVindex("unicode_loose_md5", "utf8ch", nil)
}

func TestUnicodeLooseMD5Cost(t *testing.T) {
	if charVindex.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", charVindex.Cost())
	}
}

func TestUnicodeLooseMD5String(t *testing.T) {
	if strings.Compare("utf8ch", charVindex.String()) != 0 {
		t.Errorf("String(): %s, want utf8ch", charVindex.String())
	}
}

func TestUnicodeLooseMD5Map(t *testing.T) {
	tcases := []struct {
		in, out string
	}{{
		in:  "Test",
		out: "\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5",
	}, {
		in:  "TEST",
		out: "\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5",
	}, {
		in:  "Te\u0301st",
		out: "\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5",
	}, {
		in:  "Tést",
		out: "\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5",
	}, {
		in:  "Bést",
		out: "²3.Os\xd0\aA\x02bIpo/\xb6",
	}, {
		in:  "Test ",
		out: "\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5",
	}, {
		in:  " Test",
		out: "\xa2\xe3Q\\~\x8d\xf1\xff\xd2\xcc\xfc\x11Ʊ\x9d\xd1",
	}, {
		in:  "Test\t",
		out: "\x82Em\xd8z\x9cz\x02\xb1\xc2\x05kZ\xba\xa2r",
	}, {
		in:  "TéstLooong",
		out: "\x96\x83\xe1+\x80C\f\xd4S\xf5\xdfߺ\x81ɥ",
	}, {
		in:  "T",
		out: "\xac\x0f\x91y\xf5\x1d\xb8\u007f\xe8\xec\xc0\xcf@ʹz",
	}}
	for _, tcase := range tcases {
		got, err := charVindex.(Unique).Map(nil, []sqltypes.Value{sqltypes.NewVarBinary(tcase.in)})
		if err != nil {
			t.Error(err)
		}
		out := string(got[0])
		if out != tcase.out {
			t.Errorf("Map(%#v): %#v, want %#v", tcase.in, out, tcase.out)
		}
	}
}

func TestUnicodeLooseMD5Verify(t *testing.T) {
	ids := []sqltypes.Value{sqltypes.NewVarBinary("Test"), sqltypes.NewVarBinary("TEst"), sqltypes.NewVarBinary("different")}
	ksids := [][]byte{[]byte("\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5"), []byte("\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5"), []byte("\v^۴\x01\xfdu$96\x90I\x1dd\xf1\xf5")}
	got, err := charVindex.Verify(nil, ids, ksids)
	if err != nil {
		t.Fatal(err)
	}
	want := []bool{true, true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("binaryMD5.Verify: %v, want %v", got, want)
	}
}

func TestNormalization(t *testing.T) {
	tcases := []struct {
		in, out string
	}{{
		in:  "Test",
		out: "\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  "TEST",
		out: "\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  "Te\u0301st",
		out: "\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  "Tést",
		out: "\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  "Bést",
		out: "\x16\x05\x16L\x17\xf3\x18\x16",
	}, {
		in:  "Test ",
		out: "\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  " Test",
		out: "\x01\t\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  "Test\t",
		out: "\x18\x16\x16L\x17\xf3\x18\x16\x01\x00",
	}, {
		in:  "TéstLooong",
		out: "\x18\x16\x16L\x17\xf3\x18\x16\x17\x11\x17q\x17q\x17q\x17O\x16\x91",
	}, {
		in:  "T",
		out: "\x18\x16",
	}}
	collator := newPooledCollator().(pooledCollator)
	for _, tcase := range tcases {
		norm, err := normalize(collator.col, collator.buf, []byte(tcase.in))
		if err != nil {
			t.Errorf("normalize(%#v) error: %v", tcase.in, err)
		}
		out := string(norm)
		if out != tcase.out {
			t.Errorf("normalize(%#v): %#v, want %#v", tcase.in, out, tcase.out)
		}
	}
}

func TestInvalidUnicodeNormalization(t *testing.T) {
	// These strings are known to contain invalid UTF-8.
	inputs := []string{
		"\x99\xeb\x9d\x18\xa4G\x84\x04]\x87\xf3\xc6|\xf2'F",
		"D\x86\x15\xbb\xda\b1?j\x8e\xb6h\xd2\v\xf5\x05",
		"\x8a[\xdf,\u007fĄE\x92\xd2W+\xcd\x06h\xd2",
	}
	wantErr := "invalid UTF-8"
	collator := newPooledCollator().(pooledCollator)

	for _, in := range inputs {
		// We've observed that infinite looping is a possible failure mode for the
		// collator when given invalid UTF-8, so we detect that with a timer.
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, err := normalize(collator.col, collator.buf, []byte(in))
			if err == nil {
				t.Errorf("normalize(%q) error = nil, expected error", in)
			}
			if !strings.Contains(err.Error(), wantErr) {
				t.Errorf("normalize(%q) error = %q, want %q", in, err.Error(), wantErr)
			}
		}()
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-done:
			timer.Stop()
		case <-timer.C:
			t.Errorf("invalid input caused infinite loop: %q", in)
		}
	}
}

// BenchmarkNormalizeSafe is the naive case where we create a new collator
// and buffer every time.
func BenchmarkNormalizeSafe(b *testing.B) {
	input := []byte("testing")

	for i := 0; i < b.N; i++ {
		collator := newPooledCollator().(pooledCollator)
		normalize(collator.col, collator.buf, input)
	}
}

// BenchmarkNormalizeShared is the ideal case where the collator and buffer
// are shared between iterations, assuming no concurrency.
func BenchmarkNormalizeShared(b *testing.B) {
	input := []byte("testing")
	collator := newPooledCollator().(pooledCollator)

	for i := 0; i < b.N; i++ {
		normalize(collator.col, collator.buf, input)
	}
}

// BenchmarkNormalizePooled should get us close to the performance of
// BenchmarkNormalizeShared, except that this way is safe for concurrent use.
func BenchmarkNormalizePooled(b *testing.B) {
	input := []byte("testing")

	for i := 0; i < b.N; i++ {
		collator := collatorPool.Get().(pooledCollator)
		normalize(collator.col, collator.buf, input)
		collatorPool.Put(collator)
	}
}

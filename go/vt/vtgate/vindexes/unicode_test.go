/*
Copyright 2020 The Vitess Authors.

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

package vindexes

import (
	"strings"
	"testing"
	"time"
)

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
	collator := newPooledCollator().(*pooledCollator)
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
	collator := newPooledCollator().(*pooledCollator)

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
		collator := newPooledCollator().(*pooledCollator)
		normalize(collator.col, collator.buf, input)
	}
}

// BenchmarkNormalizeShared is the ideal case where the collator and buffer
// are shared between iterations, assuming no concurrency.
func BenchmarkNormalizeShared(b *testing.B) {
	input := []byte("testing")
	collator := newPooledCollator().(*pooledCollator)

	for i := 0; i < b.N; i++ {
		normalize(collator.col, collator.buf, input)
	}
}

// BenchmarkNormalizePooled should get us close to the performance of
// BenchmarkNormalizeShared, except that this way is safe for concurrent use.
func BenchmarkNormalizePooled(b *testing.B) {
	input := []byte("testing")

	for i := 0; i < b.N; i++ {
		collator := collatorPool.Get().(*pooledCollator)
		normalize(collator.col, collator.buf, input)
		collatorPool.Put(collator)
	}
}

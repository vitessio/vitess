/*
Copyright 2016 lestrrat
Copyright 2023 The Vitess Authors.

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

package datetime

import (
	"fmt"
	"strings"
	"time"
)

func compile(ds map[byte]Spec, p string, exec func(Spec)) error {
	for l := len(p); l > 0; l = len(p) {
		// This is a really tight loop, so we don't even calls to
		// Verbatim() to cuase extra stuff
		var verbatim verbatimw

		i := strings.IndexByte(p, '%')
		if i < 0 {
			exec(&verbatimw{s: p})
			break
		}
		if i == l-1 {
			return fmt.Errorf(`stray %% at the end of pattern`)
		}

		// we found a '%'. we need the next byte to decide what to do next
		// we already know that i < l - 1
		// everything up to the i is verbatim
		if i > 0 {
			verbatim.s = p[:i]
			exec(&verbatimw{s: p[:i]})
			p = p[i:]
		}

		if spec, ok := ds[p[1]]; ok {
			exec(spec)
		} else {
			exec(&verbatimw{s: p[1:2]})
		}
		p = p[2:]
	}
	return nil
}

// Format takes the format `p` and the time `t` to produce the
// format date/time. Note that this function re-compiles the
// pattern every time it is called.
//
// If you know beforehand that you will be reusing the pattern
// within your application, consider creating a `Strftime` object
// and reusing it.
func Format(p string, t time.Time, prec uint8) ([]byte, error) {
	var dst []byte
	var ctx = newformatctx(t, prec)
	err := compile(DefaultMySQLStrftime, p, func(a Spec) {
		dst = a.format(&ctx, dst)
	})
	return dst, err
}

// Strftime is the object that represents a compiled strftime pattern
type Strftime struct {
	pattern  string
	compiled []Spec
	prec     uint8
}

// New creates a new Strftime object. If the compilation fails, then
// an error is returned in the second argument.
func New(p string, prec uint8) (*Strftime, error) {
	var list []Spec
	err := compile(DefaultMySQLStrftime, p, func(a Spec) {
		list = append(list, a)
	})
	if err != nil {
		return nil, err
	}
	return &Strftime{pattern: p, compiled: list, prec: prec}, nil
}

// Pattern returns the original pattern string
func (f *Strftime) Pattern() string {
	return f.pattern
}

func (f *Strftime) Format(t time.Time) []byte {
	return f.format(make([]byte, 0, len(f.pattern)+10), t)
}

func (f *Strftime) AppendFormat(dst []byte, t time.Time) []byte {
	return f.format(dst, t)
}

func (f *Strftime) format(b []byte, t time.Time) []byte {
	ctx := newformatctx(t, f.prec)
	for _, w := range f.compiled {
		b = w.format(&ctx, b)
	}
	return b
}

func (f *Strftime) FormatString(tm time.Time) string {
	return string(f.Format(tm))
}

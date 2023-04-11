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
)

func compile(ds map[byte]Spec, p string, exec func(Spec)) error {
	for l := len(p); l > 0; l = len(p) {
		i := strings.IndexByte(p, '%')
		if i < 0 {
			exec(&fmtVerbatim{s: p})
			break
		}
		if i == l-1 {
			return fmt.Errorf(`stray %% at the end of pattern`)
		}

		// we found a '%'. we need the next byte to decide what to do next
		// we already know that i < l - 1
		// everything up to the i is verbatim
		if i > 0 {
			exec(&fmtVerbatim{s: p[:i]})
			p = p[i:]
		}

		if spec, ok := ds[p[1]]; ok {
			if spec == nil {
				return fmt.Errorf(`unsupported format specifier: %%%c`, p[1])
			}
			exec(spec)
		} else {
			exec(&fmtVerbatim{s: p[1:2]})
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
func Format(p string, t DateTime, prec uint8) ([]byte, error) {
	var dst []byte
	err := compile(DefaultMySQLStrftime, p, func(a Spec) {
		dst = a.format(dst, t, prec)
	})
	return dst, err
}

// Strftime is the object that represents a compiled strftime pattern
type Strftime struct {
	pattern  string
	compiled []Spec
}

// New creates a new Strftime object. If the compilation fails, then
// an error is returned in the second argument.
func New(p string) (*Strftime, error) {
	var list []Spec
	err := compile(DefaultMySQLStrftime, p, func(a Spec) {
		list = append(list, a)
	})
	if err != nil {
		return nil, err
	}
	return &Strftime{pattern: p, compiled: list}, nil
}

// Pattern returns the original pattern string
func (f *Strftime) Pattern() string {
	return f.pattern
}

func (f *Strftime) Format(dt DateTime, prec uint8) []byte {
	return f.format(make([]byte, 0, len(f.pattern)+10), dt, prec)
}

func (f *Strftime) AppendFormat(dst []byte, t DateTime, prec uint8) []byte {
	return f.format(dst, t, prec)
}

func (f *Strftime) format(dst []byte, t DateTime, prec uint8) []byte {
	for _, w := range f.compiled {
		dst = w.format(dst, t, prec)
	}
	return dst
}

func (f *Strftime) FormatString(t DateTime, prec uint8) string {
	return string(f.Format(t, prec))
}

func (f *Strftime) FormatNumeric(t DateTime) (n int64) {
	for _, w := range f.compiled {
		w := w.(numeric)
		x, width := w.numeric(t)
		n = n*int64(width) + int64(x)
	}
	return n
}

func (f *Strftime) parse(s string) (DateTime, string, bool) {
	var tp timeparts
	tp.month = -1
	tp.day = -1
	tp.yday = -1

	var ok bool
	for _, w := range f.compiled {
		s, ok = w.parse(&tp, s)
		if !ok {
			return DateTime{}, "", false
		}
	}
	t, ok := tp.toDateTime()
	return t, s, ok
}

func (f *Strftime) Parse(s string) (DateTime, bool) {
	t, s, ok := f.parse(s)
	return t, ok && len(s) == 0
}

func (f *Strftime) ParseBestEffort(s string) (DateTime, bool) {
	t, _, ok := f.parse(s)
	return t, ok
}

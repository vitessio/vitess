/*
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

package json

import (
	"bytes"
	"strconv"
	"unicode/utf8"

	"vitess.io/vitess/go/hack"
)

func NewArray(vals []*Value) *Value {
	return &Value{
		a: vals,
		t: TypeArray,
	}
}

var ErrSyntax = strconv.ErrSyntax

func Unquote(s []byte) ([]byte, error) {
	n := len(s)
	if n < 2 {
		return nil, ErrSyntax
	}
	if s[0] != '"' || s[n-1] != '"' {
		return nil, ErrSyntax
	}
	s = s[1 : n-1]
	if bytes.IndexByte(s, '\n') != -1 {
		return nil, ErrSyntax
	}

	// avoid allocation if the string is trivial
	if bytes.IndexByte(s, '\\') == -1 {
		if utf8.Valid(s) {
			return s, nil
		}
	}

	// the following code is taken from strconv.Unquote (with modification)
	var runeTmp [utf8.UTFMax]byte
	buf := make([]byte, 0, 3*len(s)/2) // Try to avoid more allocations.
	for len(s) > 0 {
		// Convert []byte to string for satisfying UnquoteChar. We won't keep
		// the retured string, so it's safe to use unsafe here.
		c, multibyte, tail, err := strconv.UnquoteChar(hack.String(s), '"')
		if err != nil {
			return nil, err
		}

		// UnquoteChar returns tail as the remaining unprocess string. Because
		// we are processing []byte, we use len(tail) to get the remaining bytes
		// instead.
		s = s[len(s)-len(tail):]
		if c < utf8.RuneSelf || !multibyte {
			buf = append(buf, byte(c))
		} else {
			n = utf8.EncodeRune(runeTmp[:], c)
			buf = append(buf, runeTmp[:n]...)
		}
	}
	return buf, nil
}

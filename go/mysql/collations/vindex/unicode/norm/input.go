// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package norm

import "unicode/utf8"

type input []byte

func inputBytes(str []byte) input {
	return str
}

func (in input) skipASCII(p, max int) int {
	for ; p < max && in[p] < utf8.RuneSelf; p++ {
	}
	return p
}

func (in input) skipContinuationBytes(p int) int {
	for ; p < len(in) && !utf8.RuneStart(in[p]); p++ {
	}
	return p
}

func (in input) appendSlice(buf []byte, b, e int) []byte {
	return append(buf, in[b:e]...)
}

func (in input) copySlice(buf []byte, b, e int) int {
	return copy(buf, in[b:e])
}

func (in input) charinfoNFC(p int) (uint16, int) {
	return nfcData.lookup(in[p:])
}

func (in input) charinfoNFKC(p int) (uint16, int) {
	return nfkcData.lookup(in[p:])
}

func (in input) hangul(p int) (r rune) {
	var size int
	if !isHangul(in[p:]) {
		return 0
	}
	r, size = utf8.DecodeRune(in[p:])
	if size != hangulUTF8Size {
		return 0
	}
	return r
}

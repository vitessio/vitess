// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Note: the file data_test.go that is generated should not be checked in.
//go:generate go run maketables.go triegen.go
//go:generate go test -tags test

// Package norm contains types and functions for normalizing Unicode strings.
package norm // import "vitess.io/vitess/go/mysql/collations/vindex/unicode/norm"

import (
	"unicode/utf8"
)

// A Form denotes a canonical representation of Unicode code points.
// The Unicode-defined normalization and equivalence forms are:
//
//	NFD   Unicode Normalization Form D
//	NFKD  Unicode Normalization Form KD
//
// For a Form f, this documentation uses the notation f(x) to mean
// the bytes or string x converted to the given form.
// A position n in x is called a boundary if conversion to the form can
// proceed independently on both sides:
//
//	f(x) == append(f(x[0:n]), f(x[n:])...)
//
// References: https://unicode.org/reports/tr15/ and
// https://unicode.org/notes/tn5/.
type Form int

const (
	NFD Form = iota
	NFKD
)

// patchTail fixes a case where a rune may be incorrectly normalized
// if it is followed by illegal continuation bytes. It returns the
// patched buffer and whether the decomposition is still in progress.
func patchTail(rb *reorderBuffer) bool {
	info, p := lastRuneStart(&rb.f, rb.out)
	if p == -1 || info.size == 0 {
		return true
	}
	end := p + int(info.size)
	extra := len(rb.out) - end
	if extra > 0 {
		// Potentially allocating memory. However, this only
		// happens with ill-formed UTF-8.
		x := make([]byte, 0)
		x = append(x, rb.out[len(rb.out)-extra:]...)
		rb.out = rb.out[:end]
		decomposeToLastBoundary(rb)
		rb.doFlush()
		rb.out = append(rb.out, x...)
		return false
	}
	buf := rb.out[p:]
	rb.out = rb.out[:p]
	decomposeToLastBoundary(rb)
	if s := rb.ss.next(info); s == ssStarter {
		rb.doFlush()
		rb.ss.first(info)
	} else if s == ssOverflow {
		rb.doFlush()
		rb.insertCGJ()
		rb.ss = 0
	}
	rb.insertUnsafe(buf, 0, info)
	return true
}

func appendQuick(rb *reorderBuffer, i int) int {
	if rb.nsrc == i {
		return i
	}
	end, _ := rb.f.quickSpan(rb.src, i, rb.nsrc, true)
	rb.out = rb.src.appendSlice(rb.out, i, end)
	return end
}

// Append returns f(append(out, b...)).
// The buffer out must be nil, empty, or equal to f(out).
func (f Form) Append(out []byte, src ...byte) []byte {
	return f.doAppend(out, inputBytes(src), len(src))
}

func (f Form) doAppend(out []byte, src input, n int) []byte {
	if n == 0 {
		return out
	}
	ft := formTable[f]
	// Attempt to do a quickSpan first so we can avoid initializing the reorderBuffer.
	if len(out) == 0 {
		p, _ := ft.quickSpan(src, 0, n, true)
		out = src.appendSlice(out, 0, p)
		if p == n {
			return out
		}
		rb := reorderBuffer{f: *ft, src: src, nsrc: n, out: out, flushF: appendFlush}
		return doAppendInner(&rb, p)
	}
	rb := reorderBuffer{f: *ft, src: src, nsrc: n}
	return doAppend(&rb, out, 0)
}

func doAppend(rb *reorderBuffer, out []byte, p int) []byte {
	rb.setFlusher(out, appendFlush)
	src, n := rb.src, rb.nsrc
	doMerge := len(out) > 0
	if q := src.skipContinuationBytes(p); q > p {
		// Move leading non-starters to destination.
		rb.out = src.appendSlice(rb.out, p, q)
		p = q
		doMerge = patchTail(rb)
	}
	fd := &rb.f
	if doMerge {
		var info Properties
		if p < n {
			info = fd.info(src, p)
			if !info.BoundaryBefore() || info.nLeadingNonStarters() > 0 {
				if p == 0 {
					decomposeToLastBoundary(rb)
				}
				p = decomposeSegment(rb, p, true)
			}
		}
		if info.size == 0 {
			rb.doFlush()
			// Append incomplete UTF-8 encoding.
			return src.appendSlice(rb.out, p, n)
		}
		if rb.nrune > 0 {
			return doAppendInner(rb, p)
		}
	}
	p = appendQuick(rb, p)
	return doAppendInner(rb, p)
}

func doAppendInner(rb *reorderBuffer, p int) []byte {
	for n := rb.nsrc; p < n; {
		p = decomposeSegment(rb, p, true)
		p = appendQuick(rb, p)
	}
	return rb.out
}

// quickSpan returns a boundary n such that src[0:n] == f(src[0:n]) and
// whether any non-normalized parts were found. If atEOF is false, n will
// not point past the last segment if this segment might be become
// non-normalized by appending other runes.
func (f *formInfo) quickSpan(src input, i, end int, atEOF bool) (n int, ok bool) {
	var lastCC uint8
	ss := streamSafe(0)
	lastSegStart := i
	for n = end; i < n; {
		if j := src.skipASCII(i, n); i != j {
			i = j
			lastSegStart = i - 1
			lastCC = 0
			ss = 0
			continue
		}
		info := f.info(src, i)
		if info.size == 0 {
			if atEOF {
				// include incomplete runes
				return n, true
			}
			return lastSegStart, true
		}
		// This block needs to be before the next, because it is possible to
		// have an overflow for runes that are starters (e.g. with U+FF9E).
		switch ss.next(info) {
		case ssStarter:
			lastSegStart = i
		case ssOverflow:
			return lastSegStart, false
		case ssSuccess:
			if lastCC > info.ccc {
				return lastSegStart, false
			}
		}
		if !info.isYesD() {
			break
		}
		lastCC = info.ccc
		i += int(info.size)
	}
	if i == n {
		if !atEOF {
			n = lastSegStart
		}
		return n, true
	}
	return lastSegStart, false
}

// FirstBoundary returns the position i of the first boundary in b
// or -1 if b contains no boundary.
func (f Form) FirstBoundary(b []byte) int {
	return f.firstBoundary(inputBytes(b), len(b))
}

func (f Form) firstBoundary(src input, nsrc int) int {
	i := src.skipContinuationBytes(0)
	if i >= nsrc {
		return -1
	}
	fd := formTable[f]
	ss := streamSafe(0)
	// We should call ss.first here, but we can't as the first rune is
	// skipped already. This means FirstBoundary can't really determine
	// CGJ insertion points correctly. Luckily it doesn't have to.
	for {
		info := fd.info(src, i)
		if info.size == 0 {
			return -1
		}
		if s := ss.next(info); s != ssSuccess {
			return i
		}
		i += int(info.size)
		if i >= nsrc {
			if !info.BoundaryAfter() && !ss.isMax() {
				return -1
			}
			return nsrc
		}
	}
}

// decomposeSegment scans the first segment in src into rb. It inserts 0x034f
// (Grapheme Joiner) when it encounters a sequence of more than 30 non-starters
// and returns the number of bytes consumed from src or iShortDst or iShortSrc.
func decomposeSegment(rb *reorderBuffer, sp int, atEOF bool) int {
	// Force one character to be consumed.
	info := rb.f.info(rb.src, sp)
	if info.size == 0 {
		return 0
	}
	if s := rb.ss.next(info); s == ssStarter {
		// TODO: this could be removed if we don't support merging.
		if rb.nrune > 0 {
			goto end
		}
	} else if s == ssOverflow {
		rb.insertCGJ()
		goto end
	}
	if err := rb.insertFlush(rb.src, sp, info); err != iSuccess {
		return int(err)
	}
	for {
		sp += int(info.size)
		if sp >= rb.nsrc {
			if !atEOF && !info.BoundaryAfter() {
				return int(iShortSrc)
			}
			break
		}
		info = rb.f.info(rb.src, sp)
		if info.size == 0 {
			if !atEOF {
				return int(iShortSrc)
			}
			break
		}
		if s := rb.ss.next(info); s == ssStarter {
			break
		} else if s == ssOverflow {
			rb.insertCGJ()
			break
		}
		if err := rb.insertFlush(rb.src, sp, info); err != iSuccess {
			return int(err)
		}
	}
end:
	if !rb.doFlush() {
		return int(iShortDst)
	}
	return sp
}

// lastRuneStart returns the runeInfo and position of the last
// rune in buf or the zero runeInfo and -1 if no rune was found.
func lastRuneStart(fd *formInfo, buf []byte) (Properties, int) {
	p := len(buf) - 1
	for ; p >= 0 && !utf8.RuneStart(buf[p]); p-- {
	}
	if p < 0 {
		return Properties{}, -1
	}
	return fd.info(inputBytes(buf), p), p
}

// decomposeToLastBoundary finds an open segment at the end of the buffer
// and scans it into rb. Returns the buffer minus the last segment.
func decomposeToLastBoundary(rb *reorderBuffer) {
	fd := &rb.f
	info, i := lastRuneStart(fd, rb.out)
	if int(info.size) != len(rb.out)-i {
		// illegal trailing continuation bytes
		return
	}
	if info.BoundaryAfter() {
		return
	}
	var add [maxNonStarters + 1]Properties // stores runeInfo in reverse order
	padd := 0
	ss := streamSafe(0)
	p := len(rb.out)
	for {
		add[padd] = info
		v := ss.backwards(info)
		if v == ssOverflow {
			// Note that if we have an overflow, it the string we are appending to
			// is not correctly normalized. In this case the behavior is undefined.
			break
		}
		padd++
		p -= int(info.size)
		if v == ssStarter || p < 0 {
			break
		}
		info, i = lastRuneStart(fd, rb.out[:p])
		if int(info.size) != p-i {
			break
		}
	}
	rb.ss = ss
	// Copy bytes for insertion as we may need to overwrite rb.out.
	var buf [maxBufferSize * utf8.UTFMax]byte
	cp := buf[:copy(buf[:], rb.out[p:])]
	rb.out = rb.out[:p]
	for padd--; padd >= 0; padd-- {
		info = add[padd]
		rb.insertUnsafe(inputBytes(cp), 0, info)
		cp = cp[info.size:]
	}
}

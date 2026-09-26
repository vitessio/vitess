//go:build goexperiment.simd && amd64

package simdbench

import (
	"math/bits"
	"simd"
	"simd/archsimd"
	"strings"
)

var hasAVX2 = archsimd.X86.AVX2()

// ---------- 1. find first delim or backslash (tokenizer scanString) ----------

func indexQuoteOrBackslashScalar(s string, delim byte) int {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c == delim || c == '\\' {
			return i
		}
	}
	return -1
}


func indexQuoteOrBackslashAVX2Bytes(b []byte, delim byte) int {
	i := 0
	if hasAVX2 {
		d := archsimd.BroadcastUint8x32(delim)
		bs := archsimd.BroadcastUint8x32('\\')
		for ; i+32 <= len(b); i += 32 {
			v := archsimd.LoadUint8x32Array((*[32]byte)(b[i : i+32]))
			m := v.Equal(d).Or(v.Equal(bs)).ToBits()
			if m != 0 {
				return i + bits.TrailingZeros32(m)
			}
		}
	}
	for ; i < len(b); i++ {
		if c := b[i]; c == delim || c == '\\' {
			return i
		}
	}
	return -1
}

// Portable simd package: no movemask, so spill the mask to uint64 lanes.
func indexQuoteOrBackslashPortable(b []byte, delim byte) int {
	d := simd.BroadcastUint8s(delim)
	bs := simd.BroadcastUint8s('\\')
	n := d.Len()
	var lanes [8]uint64 // up to 512 bits
	i := 0
	for ; i+n <= len(b); i += n {
		v := simd.LoadUint8s(b[i : i+n])
		m := v.Equal(d).Or(v.Equal(bs)).ToInt8s().ToBits().ReshapeToUint64s()
		m.Store(lanes[:n/8])
		for j := 0; j < n/8; j++ {
			if lanes[j] != 0 {
				return i + j*8 + bits.TrailingZeros64(lanes[j])/8
			}
		}
	}
	for ; i < len(b); i++ {
		if c := b[i]; c == delim || c == '\\' {
			return i
		}
	}
	return -1
}

// ---------- 2. SQL escaping (sqltypes.encodeBytesSQL) ----------

var encodeMap [256]byte

const dontEscape = 255

func init() {
	for i := range encodeMap {
		encodeMap[i] = dontEscape
	}
	for k, v := range map[byte]byte{0: '0', '\'': '\'', '\b': 'b', '\n': 'n', '\r': 'r', '\t': 't', 26: 'Z', '\\': '\\'} {
		encodeMap[k] = v
	}
}

// current Vitess approach: byte-at-a-time into the builder
func escapeCurrent(val []byte, buf *strings.Builder) {
	buf.WriteByte('\'')
	for idx, ch := range val {
		if ch == '\\' && idx+1 < len(val) && (val[idx+1] == '%' || val[idx+1] == '_') {
			buf.WriteByte(ch)
			continue
		}
		if e := encodeMap[ch]; e == dontEscape {
			buf.WriteByte(ch)
		} else {
			buf.WriteByte('\\')
			buf.WriteByte(e)
		}
	}
	buf.WriteByte('\'')
}

// scalar, but copies clean runs in bulk
func escapeRuns(val []byte, buf *strings.Builder, next func([]byte) int) {
	buf.Grow(len(val) + 2)
	buf.WriteByte('\'')
	for len(val) > 0 {
		i := next(val)
		if i < 0 {
			buf.Write(val)
			break
		}
		buf.Write(val[:i])
		ch := val[i]
		if ch == '\\' && i+1 < len(val) && (val[i+1] == '%' || val[i+1] == '_') {
			buf.WriteByte(ch)
		} else if e := encodeMap[ch]; e == dontEscape {
			buf.WriteByte(ch)
		} else {
			buf.WriteByte('\\')
			buf.WriteByte(e)
		}
		val = val[i+1:]
	}
	buf.WriteByte('\'')
}

func nextEscapeScalar(b []byte) int {
	for i, c := range b {
		if encodeMap[c] != dontEscape {
			return i
		}
	}
	return -1
}

// candidates: c < 0x20 || c == '\'' || c == '\\' (superset; caller re-checks the table)
func nextEscapeAVX2(b []byte) int {
	i := 0
	if hasAVX2 {
		lim := archsimd.BroadcastUint8x32(0x20)
		q := archsimd.BroadcastUint8x32('\'')
		bs := archsimd.BroadcastUint8x32('\\')
		for ; i+32 <= len(b); i += 32 {
			v := archsimd.LoadUint8x32Array((*[32]byte)(b[i : i+32]))
			m := v.Less(lim).Or(v.Equal(q)).Or(v.Equal(bs)).ToBits()
			if m != 0 {
				return i + bits.TrailingZeros32(m)
			}
		}
	}
	for ; i < len(b); i++ {
		if encodeMap[b[i]] != dontEscape {
			return i
		}
	}
	return -1
}

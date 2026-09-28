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

package mysql

import (
	"bytes"
	"encoding/binary"
)

// This file contains the data encoding and decoding functions.

//
// Encoding methods.
//
// The same assumptions are made for all the encoding functions:
// - there is enough space to write the data in the buffer. If not, we
// will panic with out of bounds.
// - all functions start writing at 'pos' in the buffer, and return the next position.

// lenEncIntSize returns the number of bytes required to encode a
// variable-length integer.
func lenEncIntSize(i uint64) int {
	switch {
	case i < 251:
		return 1
	case i < 1<<16:
		return 3
	case i < 1<<24:
		return 4
	default:
		return 9
	}
}

func writeLenEncInt(data []byte, pos int, i uint64) int {
	// reslice at pos to avoid doing arithmetic below
	data = data[pos:]

	switch {
	case i < 251:
		data[0] = byte(i)
		return pos + 1
	case i < 1<<16:
		_ = data[2] // early bounds check
		data[0] = 0xfc
		data[1] = byte(i)
		data[2] = byte(i >> 8)
		return pos + 3
	case i < 1<<24:
		_ = data[3] // early bounds check
		data[0] = 0xfd
		data[1] = byte(i)
		data[2] = byte(i >> 8)
		data[3] = byte(i >> 16)
		return pos + 4
	default:
		_ = data[8] // early bounds check
		data[0] = 0xfe
		data[1] = byte(i)
		data[2] = byte(i >> 8)
		data[3] = byte(i >> 16)
		data[4] = byte(i >> 24)
		data[5] = byte(i >> 32)
		data[6] = byte(i >> 40)
		data[7] = byte(i >> 48)
		data[8] = byte(i >> 56)
		return pos + 9
	}
}

func lenNullString(value string) int {
	return len(value) + 1
}

func lenEOFString(value string) int {
	return len(value)
}

func writeNullString(data []byte, pos int, value string) int {
	pos += copy(data[pos:], value)
	data[pos] = 0
	return pos + 1
}

func writeEOFString(data []byte, pos int, value string) int {
	pos += copy(data[pos:], value)
	return pos
}

func writeByte(data []byte, pos int, value byte) int {
	data[pos] = value
	return pos + 1
}

func writeUint16(data []byte, pos int, value uint16) int {
	binary.LittleEndian.PutUint16(data[pos:], value)
	return pos + 2
}

func writeUint32(data []byte, pos int, value uint32) int {
	binary.LittleEndian.PutUint32(data[pos:], value)
	return pos + 4
}

func writeUint64(data []byte, pos int, value uint64) int {
	binary.LittleEndian.PutUint64(data[pos:], value)
	return pos + 8
}

func lenEncStringSize(value string) int {
	l := len(value)
	return lenEncIntSize(uint64(l)) + l
}

func writeLenEncString(data []byte, pos int, value string) int {
	pos = writeLenEncInt(data, pos, uint64(len(value)))
	return writeEOFString(data, pos, value)
}

func writeZeroes(data []byte, pos int, len int) int {
	end := pos + len
	clear(data[pos:end])
	return end
}

//
// Decoding methods.
//
// The same assumptions are made for all the decoding functions:
// - they return the decode data, the new position to read from, and ak 'ok' flag.
// - all functions start reading at 'pos' in the buffer, and return the next position.
//

func readByte(data []byte, pos int) (byte, int, bool) {
	if pos >= len(data) {
		return 0, 0, false
	}
	return data[pos], pos + 1, true
}

func readBytes(data []byte, pos int, size int) ([]byte, int, bool) {
	if pos+size-1 >= len(data) {
		return nil, 0, false
	}
	return data[pos : pos+size], pos + size, true
}

// readBytesCopy returns a copy of the bytes in the packet.
// Useful to remember contents of ephemeral packets.
func readBytesCopy(data []byte, pos int, size int) ([]byte, int, bool) {
	if pos+size-1 >= len(data) {
		return nil, 0, false
	}
	result := make([]byte, size)
	copy(result, data[pos:pos+size])
	return result, pos + size, true
}

func readNullString(data []byte, pos int) (string, int, bool) {
	end := bytes.IndexByte(data[pos:], 0)
	if end == -1 {
		return "", 0, false
	}
	return string(data[pos : pos+end]), pos + end + 1, true
}

func readEOFString(data []byte, pos int) (string, int, bool) {
	return string(data[pos:]), len(data) - pos, true
}

func readUint8(data []byte, pos int) (uint8, int, bool) {
	b, pos, ok := readByte(data, pos)
	return uint8(b), pos, ok
}

func readUint16(data []byte, pos int) (uint16, int, bool) {
	if pos+1 >= len(data) {
		return 0, 0, false
	}
	return binary.LittleEndian.Uint16(data[pos : pos+2]), pos + 2, true
}

func readUint32(data []byte, pos int) (uint32, int, bool) {
	if pos+3 >= len(data) {
		return 0, 0, false
	}
	return binary.LittleEndian.Uint32(data[pos : pos+4]), pos + 4, true
}

func readUint64(data []byte, pos int) (uint64, int, bool) {
	if pos+7 >= len(data) {
		return 0, 0, false
	}
	return binary.LittleEndian.Uint64(data[pos : pos+8]), pos + 8, true
}

// readFixedLenUint64 reads a uint64 from a fixed-length slice
// of bytes in little endian format.
// This is used for variable length fields in MySQL packets that
// are always stored in 1, 3, 4, or 9 bytes -- with the first
// byte skipped when the length is > 1 byte.
// It returns the read value and a boolean indicating if the
// read failed.
func readFixedLenUint64(data []byte) (uint64, bool) {
	switch len(data) {
	case 1: // 1 byte
		return uint64(uint8(data[0])), true
	case 3: // 2 bytes
		return uint64(binary.LittleEndian.Uint16(data[1:])), true
	case 4: // 3 bytes
		_ = data[3] // early bounds check
		return uint64(data[1]) |
			uint64(data[2])<<8 |
			uint64(data[3])<<16, true
	case 9: // 8 bytes
		return binary.LittleEndian.Uint64(data[1:]), true
	default:
		return uint64(0), false
	}
}

func readLenEncInt(data []byte, pos int) (uint64, int, bool) {
	if pos >= len(data) {
		return 0, 0, false
	}

	// reslice to avoid arithmetic below
	data = data[pos:]

	switch data[0] {
	case 0xfc:
		// Encoded in the next 2 bytes.
		if 2 >= len(data) {
			return 0, 0, false
		}
		return uint64(data[1]) |
			uint64(data[2])<<8, pos + 3, true
	case 0xfd:
		// Encoded in the next 3 bytes.
		if 3 >= len(data) {
			return 0, 0, false
		}
		return uint64(data[1]) |
			uint64(data[2])<<8 |
			uint64(data[3])<<16, pos + 4, true
	case 0xfe:
		// Encoded in the next 8 bytes.
		if 8 >= len(data) {
			return 0, 0, false
		}
		return uint64(data[1]) |
			uint64(data[2])<<8 |
			uint64(data[3])<<16 |
			uint64(data[4])<<24 |
			uint64(data[5])<<32 |
			uint64(data[6])<<40 |
			uint64(data[7])<<48 |
			uint64(data[8])<<56, pos + 9, true
	default:
		return uint64(data[0]), pos + 1, true
	}
}

func readLenEncString(data []byte, pos int) (string, int, bool) {
	size, pos, ok := readLenEncInt(data, pos)
	if !ok {
		return "", 0, false
	}
	s := int(size)
	if pos+s-1 >= len(data) {
		return "", 0, false
	}
	return string(data[pos : pos+s]), pos + s, true
}

func skipLenEncString(data []byte, pos int) (int, bool) {
	size, pos, ok := readLenEncInt(data, pos)
	if !ok {
		return 0, false
	}
	s := int(size)
	if pos+s-1 >= len(data) {
		return 0, false
	}
	return pos + s, true
}

func readLenEncStringAsBytes(data []byte, pos int) ([]byte, int, bool) {
	size, pos, ok := readLenEncInt(data, pos)
	if !ok {
		return nil, 0, false
	}
	s := int(size)
	if pos+s-1 >= len(data) {
		return nil, 0, false
	}
	return data[pos : pos+s], pos + s, true
}

func readLenEncStringAsBytesCopy(data []byte, pos int) ([]byte, int, bool) {
	size, pos, ok := readLenEncInt(data, pos)
	if !ok {
		return nil, 0, false
	}
	s := int(size)
	if pos+s-1 >= len(data) {
		return nil, 0, false
	}
	result := make([]byte, size)
	copy(result, data[pos:pos+s])
	return result, pos + s, true
}

// > encGtidData("xxx")
//
//	[07 03 05 00 03 78 78 78]
//	 |  |  |  |  |  |------|
//	 |  |  |  |  |  ^-------- "xxx"
//	 |  |  |  |  ^------------ length of rest of bytes, 3
//	 |  |  |  ^--------------- fixed 0x00
//	 |  |  ^------------------ length of rest of bytes, 5
//	 |  ^--------------------- fixed 0x03 (SESSION_TRACK_GTIDS)
//	 ^------------------------ length of rest of bytes, 7
//
// This is ultimately lenencoded strings of length encoded strings, or:
// > lenenc(0x03 + lenenc(0x00 + lenenc(data)))
func encGtidData(data string) []byte {
	const SessionTrackGtids = 0x03

	// calculate total size up front to do 1 allocation
	// encoded layout is:
	// lenenc(0x03 + lenenc(0x00 + lenenc(data)))
	dataSize := uint64(len(data))
	dataLenEncSize := uint64(lenEncIntSize(dataSize))

	wrapSize := uint64(dataSize + dataLenEncSize + 1)
	wrapLenEncSize := uint64(lenEncIntSize(wrapSize))

	totalSize := uint64(wrapSize + wrapLenEncSize + 1)
	totalLenEncSize := uint64(lenEncIntSize(totalSize))

	gtidData := make([]byte, int(totalSize+totalLenEncSize))

	pos := 0
	pos = writeLenEncInt(gtidData, pos, totalSize)

	gtidData[pos] = SessionTrackGtids
	pos++

	pos = writeLenEncInt(gtidData, pos, wrapSize)

	gtidData[pos] = 0x00
	pos++

	pos = writeLenEncInt(gtidData, pos, dataSize)
	writeEOFString(gtidData, pos, data)

	return gtidData
}

type coder struct {
	data []byte
	pos  int
}

func (d *coder) readLenEncInt() (uint64, bool) {
	res, newPos, ok := readLenEncInt(d.data, d.pos)
	d.pos = newPos
	return res, ok
}

func (d *coder) readUint16() (uint16, bool) {
	res, newPos, ok := readUint16(d.data, d.pos)
	d.pos = newPos
	return res, ok
}

func (d *coder) readByte() (byte, bool) {
	res, newPos, ok := readByte(d.data, d.pos)
	d.pos = newPos
	return res, ok
}

func (d *coder) readLenEncString() (string, bool) {
	res, newPos, ok := readLenEncString(d.data, d.pos)
	d.pos = newPos
	return res, ok
}

func (d *coder) readLenEncInfo() (string, bool) {
	res, newPos, ok := readLenEncString(d.data, d.pos)
	if ok {
		d.pos = newPos
	}
	return res, ok
}

func (d *coder) writeByte(value byte) {
	newPos := writeByte(d.data, d.pos, value)
	d.pos = newPos
}

func (d *coder) writeLenEncInt(i uint64) {
	newPos := writeLenEncInt(d.data, d.pos, i)
	d.pos = newPos
}

func (d *coder) writeUint16(value uint16) {
	newPos := writeUint16(d.data, d.pos, value)
	d.pos = newPos
}

func (d *coder) writeUint32(value uint32) {
	newPos := writeUint32(d.data, d.pos, value)
	d.pos = newPos
}

func (d *coder) writeLenEncString(value string) {
	newPos := writeLenEncString(d.data, d.pos, value)
	d.pos = newPos
}

func (d *coder) writeEOFString(value string) {
	d.pos += copy(d.data[d.pos:], value)
}

func (d *coder) writeEOFBytes(value []byte) {
	d.pos += copy(d.data[d.pos:], value)
}

/*
Copyright 2025 The Vitess Authors.

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

package sortio

import (
	"bytes"
	"fmt"
	"io"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// RowCodec encodes and decodes sqltypes.Row using the MySQL text protocol
// row format, framed with a standard 4-byte MySQL packet header.
//
// On-disk format per row:
//
//	[MySQL packet header]
//	  payload_length: uint24 (3 bytes LE) - length of the row payload
//	  sequence_id:    uint8  (1 byte)     - always 0
//	[Row payload - MySQL text protocol format]
//	  Per column: 0xfb for NULL, otherwise lenenc_int(len) + raw bytes
type RowCodec struct {
	fields []*querypb.Field
}

const (
	// packetHeaderSize is the MySQL packet header: 3 bytes length + 1 byte sequence.
	packetHeaderSize = 4
	// nullMarker is the MySQL protocol marker for NULL values.
	nullMarker = 0xfb
)

// NewRowCodec creates a RowCodec for rows described by the given fields.
func NewRowCodec(fields []*querypb.Field) *RowCodec {
	return &RowCodec{fields: fields}
}

// EncodedSize returns the on-disk encoded size for a row, used for memory accounting.
func (c *RowCodec) EncodedSize(row sqltypes.Row) int {
	size := packetHeaderSize
	for i := range c.fields {
		if i >= len(row) || row[i].IsNull() {
			size++ // 0xfb
		} else {
			l := row[i].Len()
			size += lenEncIntSize(uint64(l)) + l
		}
	}
	return size
}

// Encode writes the MySQL packet-framed row to dst.
func (c *RowCodec) Encode(dst *bytes.Buffer, row sqltypes.Row) {
	// Compute payload length
	payloadLen := 0
	for i := range c.fields {
		if i >= len(row) || row[i].IsNull() {
			payloadLen++
		} else {
			l := row[i].Len()
			payloadLen += lenEncIntSize(uint64(l)) + l
		}
	}

	// Write packet header: 3-byte LE length + 1-byte sequence (0)
	var hdr [packetHeaderSize]byte
	hdr[0] = byte(payloadLen)
	hdr[1] = byte(payloadLen >> 8)
	hdr[2] = byte(payloadLen >> 16)
	hdr[3] = 0 // sequence id
	dst.Write(hdr[:])

	// Write row payload
	for i := range c.fields {
		if i >= len(row) || row[i].IsNull() {
			dst.WriteByte(nullMarker)
		} else {
			raw := row[i].Raw()
			writeLenEncInt(dst, uint64(len(raw)))
			dst.Write(raw)
		}
	}
}

// Decode reads one MySQL packet-framed row from the reader.
func (c *RowCodec) Decode(r io.Reader) (sqltypes.Row, error) {
	// Read packet header
	var hdr [packetHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	payloadLen := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16

	// Read full payload
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("sortio: reading row payload: %w", err)
	}

	return c.ParseRow(payload)
}

// ParseRow parses a MySQL text-protocol row payload into a Row.
// Values sub-slice directly into payload (zero-copy), so payload
// must stay alive as long as the returned Row is in use.
func (c *RowCodec) ParseRow(payload []byte) (sqltypes.Row, error) {
	row := make(sqltypes.Row, len(c.fields))
	pos := 0
	for i, field := range c.fields {
		if pos >= len(payload) {
			return nil, fmt.Errorf("sortio: unexpected end of row at column %d", i)
		}
		if payload[pos] == nullMarker {
			row[i] = sqltypes.NULL
			pos++
			continue
		}
		size, newPos, ok := readLenEncInt(payload, pos)
		if !ok {
			return nil, fmt.Errorf("sortio: invalid lenenc int at column %d", i)
		}
		pos = newPos
		if pos+int(size) > len(payload) {
			return nil, fmt.Errorf("sortio: value overflows payload at column %d", i)
		}
		val := payload[pos : pos+int(size)]
		pos += int(size)
		row[i] = sqltypes.MakeTrusted(field.Type, val)
	}

	return row, nil
}

// lenEncIntSize returns the number of bytes needed to encode i as a MySQL lenenc int.
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

// writeLenEncInt writes a MySQL length-encoded integer to w.
func writeLenEncInt(w *bytes.Buffer, i uint64) {
	switch {
	case i < 251:
		w.WriteByte(byte(i))
	case i < 1<<16:
		w.WriteByte(0xfc)
		w.WriteByte(byte(i))
		w.WriteByte(byte(i >> 8))
	case i < 1<<24:
		w.WriteByte(0xfd)
		w.WriteByte(byte(i))
		w.WriteByte(byte(i >> 8))
		w.WriteByte(byte(i >> 16))
	default:
		w.WriteByte(0xfe)
		w.WriteByte(byte(i))
		w.WriteByte(byte(i >> 8))
		w.WriteByte(byte(i >> 16))
		w.WriteByte(byte(i >> 24))
		w.WriteByte(byte(i >> 32))
		w.WriteByte(byte(i >> 40))
		w.WriteByte(byte(i >> 48))
		w.WriteByte(byte(i >> 56))
	}
}

// readLenEncInt reads a MySQL length-encoded integer from data at pos.
func readLenEncInt(data []byte, pos int) (uint64, int, bool) {
	if pos >= len(data) {
		return 0, 0, false
	}
	switch data[pos] {
	case 0xfc:
		if pos+2 >= len(data) {
			return 0, 0, false
		}
		return uint64(data[pos+1]) | uint64(data[pos+2])<<8, pos + 3, true
	case 0xfd:
		if pos+3 >= len(data) {
			return 0, 0, false
		}
		return uint64(data[pos+1]) | uint64(data[pos+2])<<8 | uint64(data[pos+3])<<16, pos + 4, true
	case 0xfe:
		if pos+8 >= len(data) {
			return 0, 0, false
		}
		return uint64(data[pos+1]) | uint64(data[pos+2])<<8 | uint64(data[pos+3])<<16 |
			uint64(data[pos+4])<<24 | uint64(data[pos+5])<<32 | uint64(data[pos+6])<<40 |
			uint64(data[pos+7])<<48 | uint64(data[pos+8])<<56, pos + 9, true
	default:
		return uint64(data[pos]), pos + 1, true
	}
}

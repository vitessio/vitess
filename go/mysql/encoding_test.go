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
	"testing"
)

func TestEncLenInt(t *testing.T) {
	tests := []struct {
		value   uint64
		encoded []byte
	}{
		{0x00, []byte{0x00}},
		{0x0a, []byte{0x0a}},
		{0xfa, []byte{0xfa}},
		{0xfb, []byte{0xfc, 0xfb, 0x00}},
		{0xfc, []byte{0xfc, 0xfc, 0x00}},
		{0xfd, []byte{0xfc, 0xfd, 0x00}},
		{0xfe, []byte{0xfc, 0xfe, 0x00}},
		{0xff, []byte{0xfc, 0xff, 0x00}},
		{0x0100, []byte{0xfc, 0x00, 0x01}},
		{0x876a, []byte{0xfc, 0x6a, 0x87}},
		{0xffff, []byte{0xfc, 0xff, 0xff}},
		{0x010000, []byte{0xfd, 0x00, 0x00, 0x01}},
		{0xabcdef, []byte{0xfd, 0xef, 0xcd, 0xab}},
		{0xffffff, []byte{0xfd, 0xff, 0xff, 0xff}},
		{0x01000000, []byte{0xfe, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}},
		{0xa0a1a2a3a4a5a6a7, []byte{0xfe, 0xa7, 0xa6, 0xa5, 0xa4, 0xa3, 0xa2, 0xa1, 0xa0}},
	}
	for _, test := range tests {
		// Check lenEncIntSize first.
		if got := lenEncIntSize(test.value); got != len(test.encoded) {
			t.Errorf("lenEncIntSize returned %v but expected %v for %x", got, len(test.encoded), test.value)
		}

		// Check successful encoding.
		data := make([]byte, len(test.encoded))
		pos := writeLenEncInt(data, 0, test.value)
		if pos != len(test.encoded) {
			t.Errorf("unexpected pos %v after writeLenEncInt(%x), expected %v", pos, test.value, len(test.encoded))
		}
		if !bytes.Equal(data, test.encoded) {
			t.Errorf("unexpected encoded value for %x, got %v expected %v", test.value, data, test.encoded)
		}

		// Check successful encoding with offset.
		data = make([]byte, len(test.encoded)+1)
		pos = writeLenEncInt(data, 1, test.value)
		if pos != len(test.encoded)+1 {
			t.Errorf("unexpected pos %v after writeLenEncInt(%x, 1), expected %v", pos, test.value, len(test.encoded)+1)
		}
		if !bytes.Equal(data[1:], test.encoded) {
			t.Errorf("unexpected encoded value for %x, got %v expected %v", test.value, data, test.encoded)
		}

		// Check successful decoding.
		got, pos, ok := readLenEncInt(test.encoded, 0)
		if !ok || got != test.value || pos != len(test.encoded) {
			t.Errorf("readLenEncInt returned %x/%v/%v but expected %x/%v/%v", got, pos, ok, test.value, len(test.encoded), true)
		}

		// Check failed decoding.
		_, _, ok = readLenEncInt(test.encoded[:len(test.encoded)-1], 0)
		if ok {
			t.Errorf("readLenEncInt returned ok=true for shorter value %x", test.value)
		}
	}
}

func TestEncUint16(t *testing.T) {
	data := make([]byte, 10)

	val16 := uint16(0xabcd)

	if got := writeUint16(data, 2, val16); got != 4 {
		t.Errorf("writeUint16 returned %v but expected 4", got)
	}

	if data[2] != 0xcd || data[3] != 0xab {
		t.Errorf("writeUint16 returned bad result: %v", data)
	}

	got16, pos, ok := readUint16(data, 2)
	if !ok || got16 != val16 || pos != 4 {
		t.Errorf("readUint16 returned %v/%v/%v but expected %v/%v/%v", got16, pos, ok, val16, 4, true)
	}

	_, _, ok = readUint16(data, 9)
	if ok {
		t.Errorf("readUint16 returned ok=true for shorter value")
	}
}

func TestEncBytes(t *testing.T) {
	data := make([]byte, 10)

	if got := writeByte(data, 5, 0xab); got != 6 || data[5] != 0xab {
		t.Errorf("writeByte returned bad result: %v %v", got, data[5])
	}

	got, pos, ok := readByte(data, 5)
	if !ok || got != 0xab || pos != 6 {
		t.Errorf("readByte returned %v/%v/%v but expected %v/%v/%v", got, pos, ok, 0xab, 6, true)
	}

	_, _, ok = readByte(data, 10)
	if ok {
		t.Errorf("readByte returned ok=true for shorter value")
	}

	b, pos, ok := readBytes(data, 5, 2)
	expected := []byte{0xab, 0x00}
	if !ok || !bytes.Equal(b, expected) || pos != 7 {
		t.Errorf("readBytes returned %v/%v/%v but expected %v/%v/%v", b, pos, ok, expected, 7, true)
	}

	_, _, ok = readBytes(data, 9, 2)
	if ok {
		t.Errorf("readBytes returned ok=true for shorter value")
	}
}

func TestEncUint32(t *testing.T) {
	data := make([]byte, 10)

	val32 := uint32(0xabcdef10)

	if got := writeUint32(data, 2, val32); got != 6 {
		t.Errorf("writeUint32 returned %v but expected 6", got)
	}

	if data[2] != 0x10 || data[3] != 0xef || data[4] != 0xcd || data[5] != 0xab {
		t.Errorf("writeUint32 returned bad result: %v", data)
	}

	got32, pos, ok := readUint32(data, 2)
	if !ok || got32 != val32 || pos != 6 {
		t.Errorf("readUint32 returned %v/%v/%v but expected %v/%v/%v", got32, pos, ok, val32, 6, true)
	}

	_, _, ok = readUint32(data, 7)
	if ok {
		t.Errorf("readUint32 returned ok=true for shorter value")
	}
}

func TestEncUint64(t *testing.T) {
	data := make([]byte, 10)

	val64 := uint64(0xabcdef1011121314)

	if got := writeUint64(data, 1, val64); got != 9 {
		t.Errorf("writeUint64 returned %v but expected 9", got)
	}

	if data[1] != 0x14 || data[2] != 0x13 || data[3] != 0x12 || data[4] != 0x11 ||
		data[5] != 0x10 || data[6] != 0xef || data[7] != 0xcd || data[8] != 0xab {
		t.Errorf("writeUint64 returned bad result: %v", data)
	}

	got64, pos, ok := readUint64(data, 1)
	if !ok || got64 != val64 || pos != 9 {
		t.Errorf("readUint64 returned %v/%v/%v but expected %v/%v/%v", got64, pos, ok, val64, 6, true)
	}

	_, _, ok = readUint64(data, 7)
	if ok {
		t.Errorf("readUint64 returned ok=true for shorter value")
	}
}

func TestEncString(t *testing.T) {
	tests := []struct {
		value       string
		lenEncoded  []byte
		nullEncoded []byte
		eofEncoded  []byte
	}{
		{
			"",
			[]byte{0x00},
			[]byte{0x00},
			[]byte{},
		},
		{
			"a",
			[]byte{0x01, 'a'},
			[]byte{'a', 0x00},
			[]byte{'a'},
		},
		{
			"0123456789",
			[]byte{0x0a, '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'},
			[]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 0x00},
			[]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'},
		},
	}
	for _, test := range tests {
		// len encoded tests.

		// Check lenEncStringSize first.
		if got := lenEncStringSize(test.value); got != len(test.lenEncoded) {
			t.Errorf("lenEncStringSize returned %v but expected %v for %v", got, len(test.lenEncoded), test.value)
		}

		// Check lenNullString
		if got := lenNullString(test.value); got != len(test.nullEncoded) {
			t.Errorf("lenNullString returned %v but expected %v for %v", got, len(test.nullEncoded), test.value)
		}

		// Check lenEOFString
		if got := lenEOFString(test.value); got != len(test.eofEncoded) {
			t.Errorf("lenNullString returned %v but expected %v for %v", got, len(test.eofEncoded), test.value)
		}

		// Check successful encoding.
		data := make([]byte, len(test.lenEncoded))
		pos := writeLenEncString(data, 0, test.value)
		if pos != len(test.lenEncoded) {
			t.Errorf("unexpected pos %v after writeLenEncString(%v), expected %v", pos, test.value, len(test.lenEncoded))
		}
		if !bytes.Equal(data, test.lenEncoded) {
			t.Errorf("unexpected lenEncoded value for %v, got %v expected %v", test.value, data, test.lenEncoded)
		}

		// Check successful encoding with offset.
		data = make([]byte, len(test.lenEncoded)+1)
		pos = writeLenEncString(data, 1, test.value)
		if pos != len(test.lenEncoded)+1 {
			t.Errorf("unexpected pos %v after writeLenEncString(%v, 1), expected %v", pos, test.value, len(test.lenEncoded)+1)
		}
		if !bytes.Equal(data[1:], test.lenEncoded) {
			t.Errorf("unexpected lenEncoded value for %v, got %v expected %v", test.value, data[1:], test.lenEncoded)
		}

		// Check successful decoding as string.
		got, pos, ok := readLenEncString(test.lenEncoded, 0)
		if !ok || got != test.value || pos != len(test.lenEncoded) {
			t.Errorf("readLenEncString returned %v/%v/%v but expected %v/%v/%v", got, pos, ok, test.value, len(test.lenEncoded), true)
		}

		// Check failed decoding with shorter data.
		_, _, ok = readLenEncString(test.lenEncoded[:len(test.lenEncoded)-1], 0)
		if ok {
			t.Errorf("readLenEncString returned ok=true for shorter value %v", test.value)
		}

		// Check failed decoding with no data.
		_, _, ok = readLenEncString([]byte{}, 0)
		if ok {
			t.Errorf("readLenEncString returned ok=true for empty value %v", test.value)
		}

		// Check successful skipping as string.
		pos, ok = skipLenEncString(test.lenEncoded, 0)
		if !ok || pos != len(test.lenEncoded) {
			t.Errorf("skipLenEncString returned %v/%v but expected %v/%v", pos, ok, len(test.lenEncoded), true)
		}

		// Check failed skipping with shorter data.
		_, ok = skipLenEncString(test.lenEncoded[:len(test.lenEncoded)-1], 0)
		if ok {
			t.Errorf("skipLenEncString returned ok=true for shorter value %v", test.value)
		}

		// Check failed skipping with no data.
		_, ok = skipLenEncString([]byte{}, 0)
		if ok {
			t.Errorf("skipLenEncString returned ok=true for empty value %v", test.value)
		}

		// Check successful decoding as bytes.
		gotb, pos, ok := readLenEncStringAsBytes(test.lenEncoded, 0)
		if !ok || string(gotb) != test.value || pos != len(test.lenEncoded) {
			t.Errorf("readLenEncString returned %v/%v/%v but expected %v/%v/%v", gotb, pos, ok, test.value, len(test.lenEncoded), true)
		}

		// Check failed decoding as bytes with shorter data.
		_, _, ok = readLenEncStringAsBytes(test.lenEncoded[:len(test.lenEncoded)-1], 0)
		if ok {
			t.Errorf("readLenEncStringAsBytes returned ok=true for shorter value %v", test.value)
		}

		// Check failed decoding as bytes with no data.
		_, _, ok = readLenEncStringAsBytes([]byte{}, 0)
		if ok {
			t.Errorf("readLenEncStringAsBytes returned ok=true for empty value %v", test.value)
		}

		// null encoded tests.

		// Check successful encoding.
		data = make([]byte, len(test.nullEncoded))
		pos = writeNullString(data, 0, test.value)
		if pos != len(test.nullEncoded) {
			t.Errorf("unexpected pos %v after writeNullString(%v), expected %v", pos, test.value, len(test.nullEncoded))
		}
		if !bytes.Equal(data, test.nullEncoded) {
			t.Errorf("unexpected nullEncoded value for %v, got %v expected %v", test.value, data, test.nullEncoded)
		}

		// Check successful decoding.
		got, pos, ok = readNullString(test.nullEncoded, 0)
		if !ok || got != test.value || pos != len(test.nullEncoded) {
			t.Errorf("readNullString returned %v/%v/%v but expected %v/%v/%v", got, pos, ok, test.value, len(test.nullEncoded), true)
		}

		// Check failed decoding with shorter data.
		_, _, ok = readNullString(test.nullEncoded[:len(test.nullEncoded)-1], 0)
		if ok {
			t.Errorf("readNullString returned ok=true for shorter value %v", test.value)
		}

		// EOF encoded tests.

		// Check successful encoding.
		data = make([]byte, len(test.eofEncoded))
		pos = writeEOFString(data, 0, test.value)
		if pos != len(test.eofEncoded) {
			t.Errorf("unexpected pos %v after writeEOFString(%v), expected %v", pos, test.value, len(test.eofEncoded))
		}
		if !bytes.Equal(data, test.eofEncoded[:len(test.eofEncoded)]) {
			t.Errorf("unexpected eofEncoded value for %v, got %v expected %v", test.value, data, test.eofEncoded)
		}

		// Check successful decoding.
		got, pos, ok = readEOFString(test.eofEncoded, 0)
		if !ok || got != test.value || pos != len(test.eofEncoded) {
			t.Errorf("readEOFString returned %v/%v/%v but expected %v/%v/%v", got, pos, ok, test.value, len(test.eofEncoded), true)
		}
	}
}

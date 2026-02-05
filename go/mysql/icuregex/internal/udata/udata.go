/*
© 2016 and later: Unicode, Inc. and others.
Copyright (C) 2004-2015, International Business Machines Corporation and others.
Copyright 2023 The Vitess Authors.

This file contains code derived from the Unicode Project's ICU library.
License & terms of use for the original code: http://www.unicode.org/copyright.html

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

package udata

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

type DataInfo struct {
	/** sizeof(UDataInfo)
	 *  @stable ICU 2.0 */
	Size uint16

	/** unused, set to 0
	 *  @stable ICU 2.0*/
	ReservedWord uint16

	/* platform data properties */
	/** 0 for little-endian machine, 1 for big-endian
	 *  @stable ICU 2.0 */
	IsBigEndian uint8

	/** see U_CHARSET_FAMILY values in utypes.h
	 *  @stable ICU 2.0*/
	CharsetFamily uint8

	/** sizeof(UChar), one of { 1, 2, 4 }
	 *  @stable ICU 2.0*/
	SizeofUChar uint8

	/** unused, set to 0
	 *  @stable ICU 2.0*/
	ReservedByte uint8

	/** data format identifier
	 *  @stable ICU 2.0*/
	DataFormat [4]uint8

	/** versions: [0] major [1] minor [2] milli [3] micro
	 *  @stable ICU 2.0*/
	FormatVersion [4]uint8

	/** versions: [0] major [1] minor [2] milli [3] micro
	 *  @stable ICU 2.0*/
	DataVersion [4]uint8
}

type Bytes struct {
	buf  []byte
	orig []byte
	enc  binary.ByteOrder
}

func NewBytes(b []byte) *Bytes {
	return &Bytes{buf: b, orig: b, enc: binary.LittleEndian}
}

func (b *Bytes) ReadHeader(isValid func(info *DataInfo) bool) error {
	type MappedData struct {
		headerSize uint16
		magic1     uint8
		magic2     uint8
	}

	type DataHeader struct {
		dataHeader MappedData
		info       DataInfo
	}

	data := unsafe.SliceData(b.buf)
	header := (*DataHeader)(unsafe.Pointer(data))

	if header.dataHeader.magic1 != 0xda || header.dataHeader.magic2 != 0x27 {
		return errors.New("invalid magic number")
	}

	if header.info.IsBigEndian != 0 {
		return errors.New("unsupported: BigEndian data source")
	}

	if !isValid(&header.info) {
		return errors.New("failed to validate data header")
	}

	b.buf = b.buf[header.dataHeader.headerSize:]
	return nil
}

func (b *Bytes) Uint8() uint8 {
	u := b.buf[0]
	b.buf = b.buf[1:]
	return u
}
func (b *Bytes) Uint16() uint16 {
	u := b.enc.Uint16(b.buf)
	b.buf = b.buf[2:]
	return u
}

func (b *Bytes) Uint16Slice(size int32) []uint16 {
	s := unsafe.Slice((*uint16)(unsafe.Pointer(unsafe.SliceData(b.buf))), size)
	b.buf = b.buf[2*size:]
	return s
}

func (b *Bytes) Uint32Slice(size int32) []uint32 {
	s := unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(b.buf))), size)
	b.buf = b.buf[4*size:]
	return s
}

func (b *Bytes) Uint32() uint32 {
	u := b.enc.Uint32(b.buf)
	b.buf = b.buf[4:]
	return u
}

func (b *Bytes) Int32() int32 {
	return int32(b.Uint32())
}

func (b *Bytes) Skip(size int32) {
	b.buf = b.buf[size:]
}

func (b *Bytes) Uint8Slice(n int32) []uint8 {
	s := b.buf[:n]
	b.buf = b.buf[n:]
	return s
}

func (b *Bytes) Position() int32 {
	return int32(len(b.orig) - len(b.buf))
}

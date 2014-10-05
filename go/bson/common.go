// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bson implements encoding and decoding of BSON objects.
package bson

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"time"
)

// Pack is the BSON binary packing protocol.
// It's little endian.
var Pack = binary.LittleEndian

var (
	timeType  = reflect.TypeOf(time.Time{})
	bytesType = reflect.TypeOf([]byte(nil))
)

// Words size in bytes.
const (
	WORD32 = 4
	WORD64 = 8
)

const (
	EOO           = 0x00
	Number        = 0x01
	String        = 0x02
	Object        = 0x03
	Array         = 0x04
	Binary        = 0x05
	Undefined     = 0x06 // deprecated
	OID           = 0x07 // unsupported
	Boolean       = 0x08
	Datetime      = 0x09
	Null          = 0x0A
	Regex         = 0x0B // unsupported
	Ref           = 0x0C // deprecated
	Code          = 0x0D // unsupported
	Symbol        = 0x0E // unsupported
	CodeWithScope = 0x0F // unsupported
	Int           = 0x10
	Timestamp     = 0x11 // unsupported
	Long          = 0x12
	Ulong         = 0x3F // nonstandard extension
	MinKey        = 0xFF // unsupported
	MaxKey        = 0x7F // unsupported
)

const (
	// MAGICTAG is the tag used to embed simple types inside
	// a bson document.
	MAGICTAG = "_Val_"
)

type BsonError struct {
	Message string
}

func NewBsonError(format string, args ...interface{}) BsonError {
	return BsonError{fmt.Sprintf(format, args...)}
}

func (err BsonError) Error() string {
	return err.Message
}

func handleError(err *error) {
	if x := recover(); x != nil {
		*err = x.(BsonError)
	}
}

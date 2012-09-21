// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Utility functions for custom encoders

package bson

import (
	"code.google.com/p/vitess/go/bytes2"
)

func EncodeStringArray(buf *bytes2.ChunkedWriter, name string, values []string) {
	if values == nil {
		EncodePrefix(buf, Null, name)
	} else {
		EncodePrefix(buf, Array, name)
		lenWriter := NewLenWriter(buf)
		for i, val := range values {
			EncodePrefix(buf, Binary, Itoa(i))
			EncodeString(buf, val)
		}
		buf.WriteByte(0)
		lenWriter.RecordLen()
	}
}

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

package uprops

import (
	"fmt"
	"sync"

	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
)

var pnamesOnce sync.Once
var pnames struct {
	valueMaps []uint32
	byteTrie  []uint8
}

func valueMaps() []uint32 {
	loadPNames()
	return pnames.valueMaps
}

func byteTrie() []uint8 {
	loadPNames()
	return pnames.byteTrie
}

func loadPNames() {
	pnamesOnce.Do(func() {
		b := udata.NewBytes(icudata.PNames)
		if err := readData(b); err != nil {
			panic(err)
		}
	})
}

func readData(bytes *udata.Bytes) error {
	err := bytes.ReadHeader(func(info *udata.DataInfo) bool {
		return info.DataFormat[0] == 0x70 &&
			info.DataFormat[1] == 0x6e &&
			info.DataFormat[2] == 0x61 &&
			info.DataFormat[3] == 0x6d &&
			info.FormatVersion[0] == 2
	})
	if err != nil {
		return err
	}

	count := bytes.Int32() / 4
	if count < 8 {
		return fmt.Errorf("indexes[0] too small in ucase.icu")
	}

	indexes := make([]int32, count)
	indexes[0] = count * 4

	for i := int32(1); i < count; i++ {
		indexes[i] = bytes.Int32()
	}

	offset := indexes[ixValueMapsOffset]
	nextOffset := indexes[ixByteTriesOffset]
	numInts := (nextOffset - offset) / 4

	pnames.valueMaps = bytes.Uint32Slice(numInts)

	offset = nextOffset
	nextOffset = indexes[ixNameGroupsOffset]
	numBytes := nextOffset - offset

	pnames.byteTrie = bytes.Uint8Slice(numBytes)
	return nil
}

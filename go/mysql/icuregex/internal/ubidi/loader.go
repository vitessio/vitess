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

package ubidi

import (
	"errors"
	"sync"

	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
	"vitess.io/vitess/go/mysql/icuregex/internal/utrie"
)

var ubidiOnce sync.Once
var ubidi struct {
	indexes []int32
	trie    *utrie.UTrie2
	mirrors []uint32
	jg      []uint8
	jg2     []uint8
}

func indexes() []int32 {
	loadUBidi()
	return ubidi.indexes
}

func trie() *utrie.UTrie2 {
	loadUBidi()
	return ubidi.trie
}

func mirrors() []uint32 {
	loadUBidi()
	return ubidi.mirrors
}

func jg() []uint8 {
	loadUBidi()
	return ubidi.jg
}

func jg2() []uint8 {
	loadUBidi()
	return ubidi.jg2
}

func loadUBidi() {
	ubidiOnce.Do(func() {
		b := udata.NewBytes(icudata.UBidi)
		if err := readData(b); err != nil {
			panic(err)
		}
	})
}

func readData(bytes *udata.Bytes) error {
	err := bytes.ReadHeader(func(info *udata.DataInfo) bool {
		return info.DataFormat[0] == 0x42 &&
			info.DataFormat[1] == 0x69 &&
			info.DataFormat[2] == 0x44 &&
			info.DataFormat[3] == 0x69 &&
			info.FormatVersion[0] == 2
	})
	if err != nil {
		return err
	}

	count := int32(bytes.Uint32())
	if count < ixTop {
		return errors.New("indexes[0] too small in ucase.icu")
	}

	ubidi.indexes = make([]int32, count)
	ubidi.indexes[0] = count

	for i := int32(1); i < count; i++ {
		ubidi.indexes[i] = int32(bytes.Uint32())
	}

	ubidi.trie, err = utrie.UTrie2FromBytes(bytes)
	if err != nil {
		return err
	}

	expectedTrieLength := ubidi.indexes[ixTrieSize]
	trieLength := ubidi.trie.SerializedLength()

	if trieLength > expectedTrieLength {
		return errors.New("ucase.icu: not enough bytes for the trie")
	}

	bytes.Skip(expectedTrieLength - trieLength)

	if n := ubidi.indexes[ixMirrorLength]; n > 0 {
		ubidi.mirrors = bytes.Uint32Slice(n)
	}
	if n := ubidi.indexes[ixJgLimit] - ubidi.indexes[ixJgStart]; n > 0 {
		ubidi.jg = bytes.Uint8Slice(n)
	}
	if n := ubidi.indexes[ixJgLimit2] - ubidi.indexes[ixJgStart2]; n > 0 {
		ubidi.jg2 = bytes.Uint8Slice(n)
	}

	return nil
}

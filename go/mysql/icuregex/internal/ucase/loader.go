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

package ucase

import (
	"errors"
	"sync"

	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
	"vitess.io/vitess/go/mysql/icuregex/internal/utrie"
)

var ucaseOnce sync.Once
var ucase struct {
	trie       *utrie.UTrie2
	exceptions []uint16
	unfold     []uint16
}

func trie() *utrie.UTrie2 {
	loadUCase()
	return ucase.trie
}

func exceptions() []uint16 {
	loadUCase()
	return ucase.exceptions
}

func unfold() []uint16 {
	loadUCase()
	return ucase.unfold
}

func loadUCase() {
	ucaseOnce.Do(func() {
		b := udata.NewBytes(icudata.UCase)
		if err := readData(b); err != nil {
			panic(err)
		}
	})
}

func readData(bytes *udata.Bytes) error {
	err := bytes.ReadHeader(func(info *udata.DataInfo) bool {
		return info.DataFormat[0] == 0x63 &&
			info.DataFormat[1] == 0x41 &&
			info.DataFormat[2] == 0x53 &&
			info.DataFormat[3] == 0x45 &&
			info.FormatVersion[0] == 4
	})
	if err != nil {
		return err
	}

	count := int32(bytes.Uint32())
	if count < ixTop {
		return errors.New("indexes[0] too small in ucase.icu")
	}

	indexes := make([]int32, count)
	indexes[0] = count

	for i := int32(1); i < count; i++ {
		indexes[i] = int32(bytes.Uint32())
	}

	ucase.trie, err = utrie.UTrie2FromBytes(bytes)
	if err != nil {
		return err
	}

	expectedTrieLength := indexes[ixTrieSize]
	trieLength := ucase.trie.SerializedLength()

	if trieLength > expectedTrieLength {
		return errors.New("ucase.icu: not enough bytes for the trie")
	}

	bytes.Skip(expectedTrieLength - trieLength)

	if n := indexes[ixExcLength]; n > 0 {
		ucase.exceptions = bytes.Uint16Slice(n)
	}
	if n := indexes[ixUnfoldLength]; n > 0 {
		ucase.unfold = bytes.Uint16Slice(n)
	}

	return nil
}

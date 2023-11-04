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

package uchar

import (
	"errors"
	"sync"

	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
	"vitess.io/vitess/go/mysql/icuregex/internal/utrie"
)

var upropsOnce sync.Once
var uprops struct {
	trie             *utrie.UTrie2
	trie2            *utrie.UTrie2
	vectorsColumns   int32
	vectors          []uint32
	scriptExtensions []uint16
}

func trie() *utrie.UTrie2 {
	loadUProps()
	return uprops.trie
}

func trie2() *utrie.UTrie2 {
	loadUProps()
	return uprops.trie2
}

func vectorsColumns() int32 {
	loadUProps()
	return uprops.vectorsColumns
}

func vectors() []uint32 {
	loadUProps()
	return uprops.vectors
}

func scriptExtensions() []uint16 {
	loadUProps()
	return uprops.scriptExtensions
}

func loadUProps() {
	upropsOnce.Do(func() {
		b := udata.NewBytes(icudata.UProps)
		if err := readData(b); err != nil {
			panic(err)
		}
	})
}

func readData(bytes *udata.Bytes) error {
	err := bytes.ReadHeader(func(info *udata.DataInfo) bool {
		return info.DataFormat[0] == 0x55 &&
			info.DataFormat[1] == 0x50 &&
			info.DataFormat[2] == 0x72 &&
			info.DataFormat[3] == 0x6f &&
			info.FormatVersion[0] == 7
	})
	if err != nil {
		return err
	}

	propertyOffset := bytes.Int32()
	/* exceptionOffset = */ bytes.Int32()
	/* caseOffset = */ bytes.Int32()
	additionalOffset := bytes.Int32()
	additionalVectorsOffset := bytes.Int32()
	uprops.vectorsColumns = bytes.Int32()
	scriptExtensionsOffset := bytes.Int32()
	reservedOffset7 := bytes.Int32()
	/* reservedOffset8 = */ bytes.Int32()
	/* dataTopOffset = */ bytes.Int32()
	_ = bytes.Int32()
	_ = bytes.Int32()
	bytes.Skip((16 - 12) << 2)

	uprops.trie, err = utrie.UTrie2FromBytes(bytes)
	if err != nil {
		return err
	}

	expectedTrieLength := (propertyOffset - 16) * 4
	trieLength := uprops.trie.SerializedLength()

	if trieLength > expectedTrieLength {
		return errors.New("ucase.icu: not enough bytes for the trie")
	}

	bytes.Skip(expectedTrieLength - trieLength)
	bytes.Skip((additionalOffset - propertyOffset) * 4)

	if uprops.vectorsColumns > 0 {
		uprops.trie2, err = utrie.UTrie2FromBytes(bytes)
		if err != nil {
			return err
		}

		expectedTrieLength = (additionalVectorsOffset - additionalOffset) * 4
		trieLength = uprops.trie2.SerializedLength()

		if trieLength > expectedTrieLength {
			return errors.New("ucase.icu: not enough bytes for the trie")
		}

		bytes.Skip(expectedTrieLength - trieLength)
		uprops.vectors = bytes.Uint32Slice(scriptExtensionsOffset - additionalVectorsOffset)
	}

	if n := (reservedOffset7 - scriptExtensionsOffset) * 2; n > 0 {
		uprops.scriptExtensions = bytes.Uint16Slice(n)
	}

	return nil
}

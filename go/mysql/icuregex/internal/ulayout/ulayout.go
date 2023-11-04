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

package ulayout

import (
	"errors"
	"sync"

	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
	"vitess.io/vitess/go/mysql/icuregex/internal/utrie"
)

var inpcTrie *utrie.UcpTrie
var inscTrie *utrie.UcpTrie
var voTrie *utrie.UcpTrie

const (
	ixInpcTrieTop = 1
	ixInscTrieTop = 2
	ixVoTrieTop   = 3

	ixCount = 12
)

func InpcTrie() *utrie.UcpTrie {
	loadLayouts()
	return inpcTrie
}

func InscTrie() *utrie.UcpTrie {
	loadLayouts()
	return inscTrie
}

func VoTrie() *utrie.UcpTrie {
	loadLayouts()
	return voTrie
}

var layoutsOnce sync.Once

func loadLayouts() {
	layoutsOnce.Do(func() {
		b := udata.NewBytes(icudata.ULayout)
		if err := readData(b); err != nil {
			panic(err)
		}
	})
}

func readData(bytes *udata.Bytes) error {
	err := bytes.ReadHeader(func(info *udata.DataInfo) bool {
		return info.DataFormat[0] == 0x4c &&
			info.DataFormat[1] == 0x61 &&
			info.DataFormat[2] == 0x79 &&
			info.DataFormat[3] == 0x6f &&
			info.FormatVersion[0] == 1
	})
	if err != nil {
		return err
	}

	startPos := bytes.Position()
	indexesLength := int32(bytes.Uint32()) // inIndexes[IX_INDEXES_LENGTH]
	if indexesLength < ixCount {
		return errors.New("text layout properties data: not enough indexes")
	}
	index := make([]int32, indexesLength)
	index[0] = indexesLength
	for i := int32(1); i < indexesLength; i++ {
		index[i] = int32(bytes.Uint32())
	}

	offset := indexesLength * 4
	top := index[ixInpcTrieTop]
	trieSize := top - offset
	if trieSize >= 16 {
		inpcTrie, err = utrie.UcpTrieFromBytes(bytes)
		if err != nil {
			return err
		}
	}

	pos := bytes.Position() - startPos
	bytes.Skip(top - pos)
	offset = top
	top = index[ixInscTrieTop]
	trieSize = top - offset
	if trieSize >= 16 {
		inscTrie, err = utrie.UcpTrieFromBytes(bytes)
		if err != nil {
			return err
		}
	}

	pos = bytes.Position() - startPos
	bytes.Skip(top - pos)
	offset = top
	top = index[ixVoTrieTop]
	trieSize = top - offset
	if trieSize >= 16 {
		voTrie, err = utrie.UcpTrieFromBytes(bytes)
		if err != nil {
			return err
		}
	}
	return nil
}

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

package uemoji

import (
	"sync"

	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
	"vitess.io/vitess/go/mysql/icuregex/internal/utrie"
)

var uemojiOnce sync.Once
var uemoji struct {
	trie *utrie.UcpTrie
}

func loadUEmoji() {
	uemojiOnce.Do(func() {
		b := udata.NewBytes(icudata.UEmoji)
		if err := readData(b); err != nil {
			panic(err)
		}
	})
}

func trie() *utrie.UcpTrie {
	loadUEmoji()
	return uemoji.trie
}

func readData(bytes *udata.Bytes) error {
	err := bytes.ReadHeader(func(info *udata.DataInfo) bool {
		return info.DataFormat[0] == 0x45 &&
			info.DataFormat[1] == 0x6d &&
			info.DataFormat[2] == 0x6f &&
			info.DataFormat[3] == 0x6a &&
			info.FormatVersion[0] == 1
	})
	if err != nil {
		return err
	}

	bytes.Skip(bytes.Int32() - 4)
	uemoji.trie, err = utrie.UcpTrieFromBytes(bytes)
	if err != nil {
		return err
	}
	return nil
}

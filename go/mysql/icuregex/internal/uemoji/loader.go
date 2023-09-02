package uemoji

import (
	"errors"
	"sync"

	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
	"vitess.io/vitess/go/mysql/icuregex/internal/utrie"
)

var uemojiOnce sync.Once
var uemoji struct {
	trie        *utrie.UcpTrie
	stringTries []string
}

func loadUEmoji() {
	uemojiOnce.Do(func() {
		b := udata.NewBytes(icudata.UEmoji)
		if err := readData(b); err != nil {
			panic(err)
		}
	})
}

func stringTries() []string {
	loadUEmoji()
	return uemoji.stringTries
}

func trie() *utrie.UcpTrie {
	loadUEmoji()
	return uemoji.trie
}

const (
	ixCpTrieOffset                  = 0
	ixBasicEmojiTrieOffset          = 4
	ixRgiEmojiZwjSequenceTrieOffset = 9
)

func getStringTrieIndex(i int) int {
	return i - ixBasicEmojiTrieOffset
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

	startPos := bytes.Position()
	cpTrieOffset := bytes.Int32()
	indexesLength := cpTrieOffset / 4
	if indexesLength <= ixRgiEmojiZwjSequenceTrieOffset {
		return errors.New("not enough indexes")
	}
	inIndexes := make([]int32, indexesLength)
	inIndexes[0] = cpTrieOffset
	for i := 1; i < int(indexesLength); i++ {
		inIndexes[i] = bytes.Int32()
	}

	i := ixCpTrieOffset + 1
	nextOffset := inIndexes[i]
	uemoji.trie, err = utrie.UcpTrieFromBytes(bytes)
	if err != nil {
		return err
	}
	pos := bytes.Position() - startPos
	bytes.Skip(nextOffset - pos)
	offset := nextOffset
	nextOffset = inIndexes[ixBasicEmojiTrieOffset]
	bytes.Skip(nextOffset - offset)
	uemoji.stringTries = make([]string, getStringTrieIndex(ixRgiEmojiZwjSequenceTrieOffset)+1)
	for i = ixBasicEmojiTrieOffset; i <= ixRgiEmojiZwjSequenceTrieOffset; i++ {
		offset = inIndexes[i]
		nextOffset = inIndexes[i+1]
		if nextOffset > offset {
			uemoji.stringTries[getStringTrieIndex(i)] = string(bytes.Uint8Slice((nextOffset - offset) / 2))
		}
	}
	return nil
}

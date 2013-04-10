// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package key

import (
	"fmt"
)

type KeyError string

func NewKeyError(format string, args ...interface{}) KeyError {
	return KeyError(fmt.Sprintf(format, args...))
}

func (ke KeyError) Error() string {
	return string(ke)
}

func handleError(err *error) {
	if x := recover(); x != nil {
		*err = x.(KeyError)
	}
}

// Finds the shard that covers the given value. The returned index is between
// 0 and len(tabletKeys)-1). The tabletKeys is an ordered list of the End
// values of the KeyRange structures for the shards.
//
// This function will not check the value is under the last shard's max
// (we assume it will be empty, as checked by RebuildKeyspace)
func FindShardForValue(value string, tabletKeys []KeyspaceId) int {
	index := 0
	for index < len(tabletKeys)-1 && value >= string(tabletKeys[index]) {
		index++
	}
	return index
}

// Finds the shard that covers the given interface. The returned index
// is between 0 and len(tabletKeys)-1). The tabletKeys is an ordered
// list of the End values of the KeyRange structures for the shards.
func FindShardForKey(key interface{}, tabletKeys []KeyspaceId) (i int, err error) {
	defer handleError(&err)
	return FindShardForValue(EncodeValue(key), tabletKeys), nil
}

func EncodeValue(value interface{}) string {
	switch val := value.(type) {
	case int:
		return Uint64Key(val).String()
	case uint64:
		return Uint64Key(val).String()
	case int64:
		return Uint64Key(val).String()
	case string:
		return val
	case []byte:
		return string(val)
	}
	panic(NewKeyError("Unexpected key variable type %T", value))
}

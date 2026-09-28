// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build windows

package filelock

import (
	"errors"
)

type lockType uint32

const (
	readLock  lockType = 0
	writeLock lockType = 1
)

func lock(f File, lt lockType) error {
	return errors.New("filelock: not implemented on windows")
}

func unlock(f File) error {
	return errors.New("filelock: not implemented on windows")
}

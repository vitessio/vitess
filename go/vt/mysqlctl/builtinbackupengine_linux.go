/*
Copyright 2024 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
li*/

package mysqlctl

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"

	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// openForDirectWrite opens an existing file for writing with O_DIRECT, without truncating it.
// If the filesystem does not support O_DIRECT, the returned error has code UNIMPLEMENTED.
func openForDirectWrite(name string) (*os.File, error) {
	f, err := openFile(name, os.O_WRONLY|unix.O_DIRECT, 0o644)
	if errors.Is(err, unix.EINVAL) {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "O_DIRECT is not supported: %v", err)
	}
	return f, err
}

// fallocateFile allocates the blocks of f up to size, extending it to size if needed. The blocks
// are allocated as unwritten extents, without writing any data. If the filesystem does not support
// it, the returned error has code UNIMPLEMENTED.
func fallocateFile(f *os.File, size int64) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var fallocateErr error
	err = rc.Control(func(fd uintptr) {
		for {
			fallocateErr = unix.Fallocate(int(fd), 0, 0, size)
			if fallocateErr != unix.EINTR {
				return
			}
		}
	})
	if err != nil {
		return err
	}
	if errors.Is(fallocateErr, unix.EOPNOTSUPP) {
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "fallocate is not supported for %s: %v", f.Name(), fallocateErr)
	}
	return fallocateErr
}

// disableDirectIO clears O_DIRECT on f, so that it accepts writes that are not aligned.
func disableDirectIO(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var fcntlErr error
	err = rc.Control(func(fd uintptr) {
		var flags int
		if flags, fcntlErr = unix.FcntlInt(fd, unix.F_GETFL, 0); fcntlErr != nil {
			return
		}
		_, fcntlErr = unix.FcntlInt(fd, unix.F_SETFL, flags&^unix.O_DIRECT)
	})
	if err != nil {
		return err
	}
	return fcntlErr
}

// openForSequential opens a file and hints to the kernel that this file
// is intended to be read sequentially, setting the FADV_SEQUENTIAL flag.
// See: https://linux.die.net/man/2/fadvise
func openForSequential(name string) (*os.File, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	// XXX: beyond this path, if we error, we need to close
	// our File since we're not returning it anymore.
	fstat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if err := unix.Fadvise(int(f.Fd()), 0, fstat.Size(), unix.FADV_SEQUENTIAL); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

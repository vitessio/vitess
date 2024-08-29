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
	"os"

	"golang.org/x/sys/unix"
)

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

//go:build !linux

/*
Copyright 2026 The Vitess Authors.

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

package testfs

import (
	"errors"
	"fmt"
)

// SetStalled is a non-Linux stub for the FUSE-backed testfs control API.
func SetStalled(pid int) error {
	return unsupported(pid)
}

// SetFull is a non-Linux stub for the FUSE-backed testfs control API.
func SetFull(pid int) error {
	return unsupported(pid)
}

// Clear is a non-Linux stub for the FUSE-backed testfs control API.
func Clear(pid int) error {
	return unsupported(pid)
}

// Close is a non-Linux stub for the FUSE-backed testfs control API.
func Close(pid int) error {
	return unsupported(pid)
}

// Run is a non-Linux stub for the FUSE-backed testfs entrypoint.
func Run(mountPoint, backing string) error {
	return errors.New("testfs is Linux-only")
}

// unsupported returns a uniform error for testfs operations on non-Linux platforms.
func unsupported(pid int) error {
	return fmt.Errorf("testfs is Linux-only for pid %d", pid)
}

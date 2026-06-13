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

import "fmt"

func SetStalled(pid int) error {
	return unsupported(pid)
}

func SetFull(pid int) error {
	return unsupported(pid)
}

func Clear(pid int) error {
	return unsupported(pid)
}

func Close(pid int) error {
	return unsupported(pid)
}

func Run(mountPoint, backing string) error {
	return fmt.Errorf("testfs is Linux-only")
}

func unsupported(pid int) error {
	return fmt.Errorf("testfs is Linux-only for pid %d", pid)
}

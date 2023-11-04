/*
Copyright 2023 The Vitess Authors.

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

package sync_test

import "os"

// atomicWrite overwrites a file in such a way as to produce exactly one
// filesystem event of the type CREATE or WRITE (which are tracked by viper)
// without producing any REMOVE events.
//
// At time of writing, this produces the following on x86_64 linux:
// CREATE.
func atomicWrite(path string, data []byte) error {
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, stat.Mode()); err != nil {
		return err
	}

	return os.Rename(tmp, path)
}

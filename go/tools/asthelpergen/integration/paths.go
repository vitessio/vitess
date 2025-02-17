/*
Copyright 2025 The Vitess Authors.

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

package integration

// This file is a copy of the file go/vt/sqlparser/paths.go
// We need it here to be able to test the path accumulation of the rewriter

type ASTPath string

// nextPathOffset is an implementation of binary.Uvarint that works directly on
// the ASTPath without having to cast and allocate a byte slice
func (path ASTPath) nextPathOffset() (uint64, int) {
	var x uint64
	var s uint
	for i := 0; i < len(path); i++ {
		b := path[i]
		if b < 0x80 {
			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0
}

func (path ASTPath) nextPathStep() ASTStep {
	_ = path[1] // bounds check hint to compiler; see golang.org/issue/14808
	return ASTStep(uint16(path[1]) | uint16(path[0])<<8)
}

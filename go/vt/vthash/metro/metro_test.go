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

package metro

import (
	"testing"
)

func TestMetroHash(t *testing.T) {
	const TestString = "012345678901234567890123456789012345678901234567890123456789012"

	var TestSeed0 = [16]byte{
		0xC7, 0x7C, 0xE2, 0xBF, 0xA4, 0xED, 0x9F, 0x9B,
		0x05, 0x48, 0xB2, 0xAC, 0x50, 0x74, 0xA2, 0x97,
	}
	var TestSeed1 = [16]byte{
		0x45, 0xA3, 0xCD, 0xB8, 0x38, 0x19, 0x9D, 0x7F,
		0xBD, 0xD6, 0x8D, 0x86, 0x7A, 0x14, 0xEC, 0xEF,
	}

	var metro Metro128
	metro.Init(0)
	_, _ = metro.Write([]byte(TestString))
	if TestSeed0 != metro.Sum128() {
		t.Errorf("bad hash (seed=0)")
	}

	metro.Init(1)
	_, _ = metro.Write([]byte(TestString))
	if TestSeed1 != metro.Sum128() {
		t.Errorf("bad hash (seed=1)")
	}
}

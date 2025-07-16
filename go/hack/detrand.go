/*
Copyright 2021 The Vitess Authors.

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

package hack

import (
	_ "unsafe"
)

// DisableProtoBufRandomness disables the random insertion of whitespace characters when
// serializing Protocol Buffers in textual form (both when serializing to JSON or to ProtoText)
//
// Since the introduction of the APIv2 for Protocol Buffers, the default serializers in the
// package insert random whitespace characters that don't change the meaning of the serialized
// code but make byte-wise comparison impossible. The rationale behind this decision is as follows:
//
// "The ProtoBuf authors believe that golden tests are Wrong"
//
// Fine. Unfortunately, Vitess makes extensive use of golden tests through its test suite, which
// expect byte-wise comparison to be stable between test runs. Using the new version of the
// package would require us to rewrite hundreds of tests, or alternatively, we could disable
// the randomness and call it a day. The method required to disable the randomness is not public, but
// that won't stop us because we're good at computers.
//
// Tracking issue: https://github.com/golang/protobuf/issues/1121
//
//go:linkname DisableProtoBufRandomness google.golang.org/protobuf/internal/detrand.Disable
func DisableProtoBufRandomness()

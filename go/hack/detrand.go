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

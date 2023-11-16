//go:build amd64 && !gccgo && !appengine && !nacl && !noasm
// +build amd64,!gccgo,!appengine,!nacl,!noasm

/*
Copyright (c) 2017 Minio Inc. All rights reserved.

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

package highway

import "golang.org/x/sys/cpu"

var (
	useSSE4 = cpu.X86.HasSSE41
	useAVX2 = cpu.X86.HasAVX2
	useNEON = false
	useVMX  = false
)

//go:noescape
func initializeSSE4(state *[16]uint64, key []byte)

//go:noescape
func initializeAVX2(state *[16]uint64, key []byte)

//go:noescape
func updateSSE4(state *[16]uint64, msg []byte)

//go:noescape
func updateAVX2(state *[16]uint64, msg []byte)

//go:noescape
func finalizeSSE4(out []byte, state *[16]uint64)

//go:noescape
func finalizeAVX2(out []byte, state *[16]uint64)

func initialize(state *[16]uint64, key []byte) {
	switch {
	case useAVX2:
		initializeAVX2(state, key)
	case useSSE4:
		initializeSSE4(state, key)
	default:
		initializeGeneric(state, key)
	}
}

func update(state *[16]uint64, msg []byte) {
	switch {
	case useAVX2:
		updateAVX2(state, msg)
	case useSSE4:
		updateSSE4(state, msg)
	default:
		updateGeneric(state, msg)
	}
}

func finalize(out []byte, state *[16]uint64) {
	switch {
	case useAVX2:
		finalizeAVX2(out, state)
	case useSSE4:
		finalizeSSE4(out, state)
	default:
		finalizeGeneric(out, state)
	}
}

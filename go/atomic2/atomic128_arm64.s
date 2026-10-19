// Copyright 2023 The Vitess Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//+build !noasm,!appengine

#include "textflag.h"

// CASPALD(s, t, n) encodes CASPAL R<s>, R<s+1>, R<t>, R<t+1>, [R<n>],
// the acquire/release variant of the 128-bit compare-and-swap.
//
// Go's built-in atomics are sequentially consistent, and callers of
// this package expect the same semantics; the Go runtime implements
// its own Cas64 on arm64 with CASALD, the acquire/release variant of
// the single-doubleword CAS. The plain CASP instead has relaxed
// memory ordering: its store can become visible to other CPUs before
// earlier stores by this CPU (e.g. a node's fields written before the
// node is published via this CAS), which breaks the lock-free data
// structures built on top of this primitive. Go's assembler only
// knows the relaxed CASPD/CASPW mnemonics, so encode the instruction
// directly. s and t must be even.
#define CASPALD(s, t, n) WORD $(0x4860FC00 | ((s)<<16) | ((n)<<5) | (t))

TEXT ·compareAndSwapUint128_(SB), NOSPLIT, $0-41
	MOVD addr+0(FP), R5
	MOVD oldp+8(FP), R0
	MOVD oldu+16(FP), R1
	MOVD newp+24(FP), R2
	MOVD newu+32(FP), R3
	MOVD R0, R6
	MOVD R1, R7
	CASPALD(0, 2, 5)
	CMP R0, R6
	CCMP EQ, R1, R7, $0
	CSET EQ, R0
	MOVB R0, swapped+40(FP)
	RET

TEXT ·loadUint128_(SB), NOSPLIT, $0-24
	MOVD addr+0(FP), R3
	LDAXP (R3), (R0, R1)
	MOVD R0, pp+8(FP)
	MOVD R1, uu+16(FP)
	RET

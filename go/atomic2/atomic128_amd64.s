// Copyright 2023 The Vitess Authors.
// Copyright (c) 2021, Carlo Alberto Ferraris
// Copyright (c) 2017, Tom Thorogood
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

// Use of this source code is governed by a
// Modified BSD License that can be found in
// the LICENSE file.

//+build !noasm,!appengine

#include "textflag.h"

TEXT ·compareAndSwapUint128_(SB), NOSPLIT, $0-41
	MOVQ addr+0(FP), R8
	MOVQ oldp+8(FP), AX
	MOVQ oldu+16(FP), DX
	MOVQ newp+24(FP), BX
	MOVQ newu+32(FP), CX
	LOCK
	CMPXCHG16B (R8)
	SETEQ swapped+40(FP)
	RET

TEXT ·loadUint128_(SB), NOSPLIT, $0-24
	MOVQ addr+0(FP), R8
	XORQ AX, AX
	XORQ DX, DX
	XORQ BX, BX
	XORQ CX, CX
	LOCK
	CMPXCHG16B (R8)
	MOVQ AX, pp+8(FP)
	MOVQ DX, uu+16(FP)
	RET

// Copyright (c) 2017, Tom Thorogood
// Copyright (c) 2021, Carlo Alberto Ferraris
// All rights reserved.
// Use of this source code is governed by a
// Modified BSD License that can be found in
// the LICENSE file.

// +build amd64,!gccgo,!appengine

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

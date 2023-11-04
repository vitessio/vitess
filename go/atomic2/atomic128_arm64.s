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

TEXT ·compareAndSwapUint128_(SB), NOSPLIT, $0-41
	MOVD addr+0(FP), R5
	MOVD oldp+8(FP), R0
	MOVD oldu+16(FP), R1
	MOVD newp+24(FP), R2
	MOVD newu+32(FP), R3
	MOVD R0, R6
	MOVD R1, R7
	CASPD (R0, R1), (R5), (R2, R3)
	CMP R0, R6
	CCMP EQ, R1, R7, $0
	CSET EQ, R0
	MOVB R0, ret+40(FP)
	RET

TEXT ·loadUint128_(SB), NOSPLIT, $0-24
	MOVD addr+0(FP), R3
	LDAXP (R3), (R0, R1)
	MOVD R0, val+8(FP)
	MOVD R1, val+16(FP)
	RET

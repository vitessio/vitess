/*
© 2016 and later: Unicode, Inc. and others.
Copyright (C) 2004-2015, International Business Machines Corporation and others.
Copyright 2023 The Vitess Authors.

This file contains code derived from the Unicode Project's ICU library.
License & terms of use for the original code: http://www.unicode.org/copyright.html

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

package icuregex

import (
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/mysql/icuregex/internal/ucase"
	"vitess.io/vitess/go/mysql/icuregex/internal/utf16"
)

type opcode uint8

const (
	urxReservedOp opcode = iota // For multi-operand ops, most non-first words.
	urxBacktrack                // Force a backtrack, as if a match test had failed.
	urxEnd
	urxOnechar   // Value field is the 21 bit unicode char to match
	urxString    // Value field is index of string start
	urxStringLen // Value field is string length (code units)
	urxStateSave // Value field is pattern position to push
	urxNop
	urxStartCapture // Value field is capture group number.
	urxEndCapture   // Value field is capture group number
	urxStaticSetref // Value field is index of set in array of sets.
	urxSetref       // Value field is index of set in array of sets.
	urxDotany
	urxJmp  // Value field is destination position in the pattern.
	urxFail // Stop match operation,  No match.

	urxJmpSav     // Operand:  JMP destination location
	urxBackslashB // Value field:  0:  \b    1:  \B
	urxBackslashG
	urxJmpSavX // Conditional JMP_SAV,
	//    Used in (x)+, breaks loop on zero length match.
	//    Operand:  Jmp destination.
	urxBackslashX
	urxBackslashZ // \z   Unconditional end of line.

	urxDotanyAll  // ., in the . matches any mode.
	urxBackslashD // Value field:  0:  \d    1:  \D
	urxCaret      // Value field:  1:  multi-line mode.
	urxDollar     // Also for \Z

	urxCtrInit   // Counter Inits for {Interval} loops.
	urxCtrInitNg //   2 kinds, normal and non-greedy.
	//   These are 4 word opcodes.  See description.
	//    First Operand:  Data loc of counter variable
	//    2nd   Operand:  Pat loc of the URX_CTR_LOOPx
	//                    at the end of the loop.
	//    3rd   Operand:  Minimum count.
	//    4th   Operand:  Max count, -1 for unbounded.

	urxDotanyUnix // '.' operator in UNIX_LINES mode, only \n marks end of line.

	utxCtrLoop   // Loop Ops for {interval} loops.
	urxCtrLoopNg //   Also in three flavors.
	//   Operand is loc of corresponding CTR_INIT.

	urxCaretMUnix // '^' operator, test for start of line in multi-line
	//      plus UNIX_LINES mode.

	urxRelocOprnd // Operand value in multi-operand ops that refers
	//   back into compiled pattern code, and thus must
	//   be relocated when inserting/deleting ops in code.

	urxStoSp // Store the stack ptr.  Operand is location within
	//   matcher data (not stack data) to store it.
	urxLdSp // Load the stack pointer.  Operand is location
	//   to load from.
	urxBackref // Back Reference.  Parameter is the index of the
	//   capture group variables in the state stack frame.
	urxStoInpLoc // Store the input location.  Operand is location
	//   within the matcher stack frame.
	urxJmpx // Conditional JMP.
	//   First Operand:  JMP target location.
	//   Second Operand:  Data location containing an
	//     input position.  If current input position ==
	//     saved input position, FAIL rather than taking
	//     the JMP
	urxLaStart // Starting a LookAround expression.
	//   Save InputPos, SP and active region in static data.
	//   Operand:  Static data offset for the save
	urxLaEnd // Ending a Lookaround expression.
	//   Restore InputPos and Stack to saved values.
	//   Operand:  Static data offset for saved data.
	urcOnecharI // Test for case-insensitive match of a literal character.
	//   Operand:  the literal char.
	urxStringI // Case insensitive string compare.
	//   First Operand:  Index of start of string in string literals
	//   Second Operand (next word in compiled code):
	//     the length of the string.
	urxBackrefI // Case insensitive back reference.
	//   Parameter is the index of the
	//   capture group variables in the state stack frame.
	urxDollarM // $ in multi-line mode.
	urxCaretM  // ^ in multi-line mode.
	urxLbStart // LookBehind Start.
	//   Parameter is data location
	urxLbCont // LookBehind Continue.
	//   Param 0:  the data location
	//   Param 1:  The minimum length of the look-behind match
	//   Param 2:  The max length of the look-behind match
	urxLbEnd // LookBehind End.
	//   Parameter is the data location.
	//     Check that match ended at the right spot,
	//     Restore original input string len.
	urxLbnCount // Negative LookBehind Continue
	//   Param 0:  the data location
	//   Param 1:  The minimum length of the look-behind match
	//   Param 2:  The max     length of the look-behind match
	//   Param 3:  The pattern loc following the look-behind block.
	urxLbnEnd // Negative LookBehind end
	//   Parameter is the data location.
	//   Check that the match ended at the right spot.
	urxStatSetrefN // Reference to a prebuilt set (e.g. \w), negated
	//   Operand is index of set in array of sets.
	urxLoopSrI // Init a [set]* loop.
	//   Operand is the sets index in array of user sets.
	urxLoopC // Continue a [set]* or OneChar* loop.
	//   Operand is a matcher static data location.
	//   Must always immediately follow  LOOP_x_I instruction.
	urxLoopDotI // .*, initialization of the optimized loop.
	//   Operand value:
	//      bit 0:
	//         0:  Normal (. doesn't match new-line) mode.
	//         1:  . matches new-line mode.
	//      bit 1:  controls what new-lines are recognized by this operation.
	//         0:  All Unicode New-lines
	//         1:  UNIX_LINES, \u000a only.
	urxBackslashBu // \b or \B in UREGEX_UWORD mode, using Unicode style
	//   word boundaries.
	urxDollarD    // $ end of input test, in UNIX_LINES mode.
	urxDollarMd   // $ end of input test, in MULTI_LINE and UNIX_LINES mode.
	urxBackslashH // Value field:  0:  \h    1:  \H
	urxBackslashR // Any line break sequence.
	urxBackslashV // Value field:  0:  \v    1:  \V

	urxReservedOpN opcode = 255 // For multi-operand ops, negative operand values.
)

// Keep this list of opcode names in sync with the above enum
//
//	Used for debug printing only.
var urxOpcodeNames = []string{
	"               ",
	"BACKTRACK",
	"END",
	"ONECHAR",
	"STRING",
	"STRING_LEN",
	"STATE_SAVE",
	"NOP",
	"START_CAPTURE",
	"END_CAPTURE",
	"URX_STATIC_SETREF",
	"SETREF",
	"DOTANY",
	"JMP",
	"FAIL",
	"JMP_SAV",
	"BACKSLASH_B",
	"BACKSLASH_G",
	"JMP_SAV_X",
	"BACKSLASH_X",
	"BACKSLASH_Z",
	"DOTANY_ALL",
	"BACKSLASH_D",
	"CARET",
	"DOLLAR",
	"CTR_INIT",
	"CTR_INIT_NG",
	"DOTANY_UNIX",
	"CTR_LOOP",
	"CTR_LOOP_NG",
	"URX_CARET_M_UNIX",
	"RELOC_OPRND",
	"STO_SP",
	"LD_SP",
	"BACKREF",
	"STO_INP_LOC",
	"JMPX",
	"LA_START",
	"LA_END",
	"ONECHAR_I",
	"STRING_I",
	"BACKREF_I",
	"DOLLAR_M",
	"CARET_M",
	"LB_START",
	"LB_CONT",
	"LB_END",
	"LBN_CONT",
	"LBN_END",
	"STAT_SETREF_N",
	"LOOP_SR_I",
	"LOOP_C",
	"LOOP_DOT_I",
	"BACKSLASH_BU",
	"DOLLAR_D",
	"DOLLAR_MD",
	"URX_BACKSLASH_H",
	"URX_BACKSLASH_R",
	"URX_BACKSLASH_V",
}

type instruction int32

func (ins instruction) typ() opcode {
	return opcode(uint32(ins) >> 24)
}

func (ins instruction) value32() int32 {
	return int32(ins) & 0xffffff
}

func (ins instruction) value() int {
	return int(ins.value32())
}

// Access to Unicode Sets composite character properties
//
//	The sets are accessed by the match engine for things like \w (word boundary)
const (
	urxIswordSet  = 1
	urxIsalnumSet = 2
	urxIsalphaSet = 3
	urxIsspaceSet = 4

	urxGcNormal = iota + 1 // Sets for finding grapheme cluster boundaries.
	urxGcExtend
	urxGcControl
	urxGcL
	urxGcLv
	urxGcLvt
	urxGcV
	urxGcT

	urxNegSet = 0x800000 // Flag bit to reverse sense of set
	//   membership test.
)

type stack struct {
	ary        []int
	frameSize  int
	stackLimit int
}

type stackFrame []int

func (f stackFrame) inputIdx() *int {
	return &f[0]
}

func (f stackFrame) patIdx() *int {
	return &f[1]
}

func (f stackFrame) extra(n int) *int {
	return &f[2+n]
}

func (f stackFrame) equals(f2 stackFrame) bool {
	return &f[0] == &f2[0]
}

func (s *stack) len() int {
	return len(s.ary)
}

func (s *stack) sp() int {
	return len(s.ary) - s.frameSize
}

func (s *stack) newFrame(inputIdx int, input []rune, pattern string) (stackFrame, error) {
	if s.stackLimit != 0 && len(s.ary)+s.frameSize > s.stackLimit {
		return nil, &MatchError{
			Code:     StackOverflow,
			Pattern:  pattern,
			Position: inputIdx,
			Input:    input,
		}
	}
	s.ary = slices.Grow(s.ary, s.frameSize)

	f := s.ary[len(s.ary) : len(s.ary)+s.frameSize]
	s.ary = s.ary[:len(s.ary)+s.frameSize]
	return f, nil
}

func (s *stack) prevFromTop() stackFrame {
	return s.ary[len(s.ary)-2*s.frameSize:]
}

func (s *stack) popFrame() stackFrame {
	s.ary = s.ary[:len(s.ary)-s.frameSize]
	return s.ary[len(s.ary)-s.frameSize:]
}

func (s *stack) reset() {
	s.ary = s.ary[:0]
}

func (s *stack) offset(size int) stackFrame {
	return s.ary[size-s.frameSize : size]
}

func (s *stack) setSize(size int) {
	s.ary = s.ary[:size]
}

func (f stackFrame) clearExtra() {
	for i := 2; i < len(f); i++ {
		f[i] = -1
	}
}

// number of UVector elements in the header
const restackframeHdrCount = 2

// Start-Of-Match type.  Used by find() to quickly scan to positions where a
//
//	match might start before firing up the full match engine.
type startOfMatch int8

const (
	startNoInfo startOfMatch = iota // No hint available.
	startChar                       // Match starts with a literal code point.
	startSet                        // Match starts with something matching a set.
	startStart                      // Match starts at start of buffer only (^ or \A)
	startLine                       // Match starts with ^ in multi-line mode.
	startString                     // Match starts with a literal string.
)

func (som startOfMatch) String() string {
	switch som {
	case startNoInfo:
		return "START_NO_INFO"
	case startChar:
		return "START_CHAR"
	case startSet:
		return "START_SET"
	case startStart:
		return "START_START"
	case startLine:
		return "START_LINE"
	case startString:
		return "START_STRING"
	default:
		panic("unknown StartOfMatch")
	}
}

type caseFoldIterator struct {
	chars []rune
	index int
	limit int

	foldChars []uint16
}

func (it *caseFoldIterator) next() rune {
	if len(it.foldChars) == 0 {
		// We are not in a string folding of an earlier character.
		// Start handling the next char from the input UText.
		if it.index >= it.limit {
			return -1
		}

		originalC := it.chars[it.index]
		it.index++

		originalC, it.foldChars = ucase.FullFolding(originalC)
		if len(it.foldChars) == 0 {
			// input code point folds to a single code point, possibly itself.
			return originalC
		}
	}

	var res rune
	res, it.foldChars = utf16.NextUnsafe(it.foldChars)
	return res
}

func (it *caseFoldIterator) inExpansion() bool {
	return len(it.foldChars) > 0
}

func newCaseFoldIterator(chars []rune, start, limit int) caseFoldIterator {
	return caseFoldIterator{
		chars: chars,
		index: start,
		limit: limit,
	}
}

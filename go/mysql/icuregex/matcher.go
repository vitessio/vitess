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
	"fmt"
	"io"

	"vitess.io/vitess/go/mysql/icuregex/internal/ucase"
	"vitess.io/vitess/go/mysql/icuregex/internal/uchar"
	"vitess.io/vitess/go/mysql/icuregex/internal/uprops"
)

const timerInitialValue = 10000
const defaultTimeout = 3
const defaultStackLimit = 0

type Matcher struct {
	pattern *Pattern

	input []rune

	regionStart int // Start of the input region, default = 0.
	regionLimit int // End of input region, default to input.length.

	anchorStart int // Region bounds for anchoring operations (^ or $).
	anchorLimit int //   See useAnchoringBounds

	lookStart int // Region bounds for look-ahead/behind and
	lookLimit int //   and other boundary tests.  See
	//   useTransparentBounds

	activeStart int // Currently active bounds for matching.
	activeLimit int //   Usually is the same as region, but
	//   is changed to fLookStart/Limit when
	//   entering look around regions.

	match      bool // True if the last attempted match was successful.
	matchStart int  // Position of the start of the most recent match
	matchEnd   int  // First position after the end of the most recent match
	//   Zero if no previous match, even when a region
	//   is active.
	lastMatchEnd int // First position after the end of the previous match,
	//   or -1 if there was no previous match.
	appendPosition int // First position after the end of the previous
	//   appendReplacement().  As described by the
	//   JavaDoc for Java Matcher, where it is called
	//   "append position"
	hitEnd     bool // True if the last match touched the end of input.
	requireEnd bool // True if the last match required end-of-input
	//    (matched $ or Z)

	stack stack
	frame stackFrame // After finding a match, the last active stack frame,
	//   which will contain the capture group results.
	//   NOT valid while match engine is running.

	data []int // Data area for use by the compiled pattern.

	timeLimit int32 // Max time (in arbitrary steps) to let the
	//   match engine run.  Zero for unlimited.

	time        int32 // Match time, accumulates while matching.
	tickCounter int32 // Low bits counter for time.  Counts down StateSaves.
	//   Kept separately from fTime to keep as much
	//   code as possible out of the inline
	//   StateSave function.

	dumper io.Writer
}

func NewMatcher(pat *Pattern) *Matcher {
	m := &Matcher{
		pattern: pat,
		data:    make([]int, pat.dataSize),
		stack: stack{
			frameSize:  pat.frameSize,
			stackLimit: defaultStackLimit,
		},
		timeLimit: defaultTimeout,
	}
	m.reset()
	return m
}

func (m *Matcher) MatchAt(startIdx int, toEnd bool) error {
	//--------------------------------------------------------------------------------
	//
	//   MatchAt      This is the actual matching engine.
	//
	//                  startIdx:    begin matching a this index.
	//                  toEnd:       if true, match must extend to end of the input region
	//
	//--------------------------------------------------------------------------------
	var err error
	var isMatch bool // True if the we have a match.

	if m.dumper != nil {
		fmt.Fprintf(m.dumper, "MatchAt(startIdx=%d)\n", startIdx)
		fmt.Fprintf(m.dumper, "Original Pattern: \"%s\"\n", m.pattern.pattern)
		fmt.Fprintf(m.dumper, "Input String:     \"%s\"\n\n", string(m.input))
	}

	pat := m.pattern.compiledPat
	inputText := m.input
	litText := m.pattern.literalText
	sets := m.pattern.sets

	fp := m.resetStack()
	*fp.inputIdx() = startIdx
	*fp.patIdx() = 0
	for i := 0; i < len(m.data); i++ {
		m.data[i] = 0
	}

	for {
		op := pat[*fp.patIdx()]

		if m.dumper != nil {
			fmt.Fprintf(m.dumper, "inputIdx=%d   inputChar=%x   sp=%3d   activeLimit=%d ", *fp.inputIdx(),
				charAt(inputText, *fp.inputIdx()), m.stack.sp(), m.activeLimit)
			m.pattern.dumpOp(m.dumper, *fp.patIdx())
		}

		*fp.patIdx()++

		switch op.typ() {
		case urxNop:
			// Nothing to do.
		case urxBacktrack:
			// Force a backtrack.  In some circumstances, the pattern compiler
			//   will notice that the pattern can't possibly match anything, and will
			//   emit one of these at that point.
			fp = m.stack.popFrame()
		case urxOnechar:
			if *fp.inputIdx() < m.activeLimit {
				c := charAt(inputText, *fp.inputIdx())
				*fp.inputIdx()++
				if c == rune(op.value()) {
					break
				}
			} else {
				m.hitEnd = true
			}
			fp = m.stack.popFrame()
		case urxString:
			// Test input against a literal string.
			// Strings require two slots in the compiled pattern, one for the
			//   offset to the string text, and one for the length.
			stringStartIdx := op.value()
			nextOp := pat[*fp.patIdx()] // Fetch the second operand
			*fp.patIdx()++
			stringLen := nextOp.value()

			patternString := litText[stringStartIdx:]
			var patternStringIndex int
			success := true
			for patternStringIndex < stringLen {
				if *fp.inputIdx() >= m.activeLimit {
					m.hitEnd = true
					success = false
					break
				}
				if charAt(patternString, patternStringIndex) != charAt(inputText, *fp.inputIdx()) {
					success = false
					break
				}
				patternStringIndex++
				*fp.inputIdx()++
			}

			if !success {
				fp = m.stack.popFrame()
			}
		case urxStateSave:
			fp, err = m.stateSave(*fp.inputIdx(), op.value())
			if err != nil {
				return err
			}
		case urxEnd:
			// The match loop will exit via this path on a successful match,
			//   when we reach the end of the pattern.
			if toEnd && *fp.inputIdx() != m.activeLimit {
				// The pattern matched, but not to the end of input.  Try some more.
				fp = m.stack.popFrame()
				break
			}
			isMatch = true
			goto breakFromLoop

		// Start and End Capture stack frame variables are laid out out like this:
		//  fp->fExtra[opValue]  - The start of a completed capture group
		//             opValue+1 - The end   of a completed capture group
		//             opValue+2 - the start of a capture group whose end
		//                          has not yet been reached (and might not ever be).
		case urxStartCapture:
			*fp.extra(op.value() + 2) = *fp.inputIdx()
		case urxEndCapture:
			*fp.extra(op.value()) = *fp.extra(op.value() + 2) // Tentative start becomes real.
			*fp.extra(op.value() + 1) = *fp.inputIdx()        // End position

		case urxDollar: //  $, test for End of line
			if *fp.inputIdx() < m.anchorLimit-2 {
				fp = m.stack.popFrame()
				break
			}
			//     or for position before new line at end of input
			if *fp.inputIdx() >= m.anchorLimit {
				// We really are at the end of input.  Success.
				m.hitEnd = true
				m.requireEnd = true
				break
			}

			if *fp.inputIdx() == m.anchorLimit-1 {
				c := m.input[*fp.inputIdx()]
				if isLineTerminator(c) {
					if !(c == 0x0a && *fp.inputIdx() > m.anchorStart && m.input[*fp.inputIdx()-1] == 0x0d) {
						// At new-line at end of input. Success
						m.hitEnd = true
						m.requireEnd = true
						break
					}
				}
			} else if *fp.inputIdx() == m.anchorLimit-2 && m.input[*fp.inputIdx()] == 0x0d && m.input[*fp.inputIdx()+1] == 0x0a {
				m.hitEnd = true
				m.requireEnd = true
				break // At CR/LF at end of input.  Success
			}
			fp = m.stack.popFrame()

		case urxDollarD: //  $, test for End of Line, in UNIX_LINES mode.
			if *fp.inputIdx() >= m.anchorLimit {
				// Off the end of input.  Success.
				m.hitEnd = true
				m.requireEnd = true
				break
			}
			c := charAt(inputText, *fp.inputIdx())
			*fp.inputIdx()++
			// Either at the last character of input, or off the end.
			if c == 0x0a && *fp.inputIdx() == m.anchorLimit {
				m.hitEnd = true
				m.requireEnd = true
				break
			}

			// Not at end of input.  Back-track out.
			fp = m.stack.popFrame()
		case urxDollarM: //  $, test for End of line in multi-line mode
			if *fp.inputIdx() >= m.anchorLimit {
				// We really are at the end of input.  Success.
				m.hitEnd = true
				m.requireEnd = true
				break
			}
			// If we are positioned just before a new-line, succeed.
			// It makes no difference where the new-line is within the input.
			c := charAt(inputText, *fp.inputIdx())
			if isLineTerminator(c) {
				// At a line end, except for the odd chance of  being in the middle of a CR/LF sequence
				//  In multi-line mode, hitting a new-line just before the end of input does not
				//   set the hitEnd or requireEnd flags
				if !(c == 0x0a && *fp.inputIdx() > m.anchorStart && charAt(inputText, *fp.inputIdx()-1) == 0x0d) {
					break
				}
			}
			// not at a new line.  Fail.
			fp = m.stack.popFrame()
		case urxDollarMd: //  $, test for End of line in multi-line and UNIX_LINES mode
			if *fp.inputIdx() >= m.anchorLimit {
				// We really are at the end of input.  Success.
				m.hitEnd = true
				m.requireEnd = true // Java set requireEnd in this case, even though
				break               //   adding a new-line would not lose the match.
			}
			// If we are not positioned just before a new-line, the test fails; backtrack out.
			// It makes no difference where the new-line is within the input.
			if charAt(inputText, *fp.inputIdx()) != 0x0a {
				fp = m.stack.popFrame()
			}
		case urxCaret: //  ^, test for start of line
			if *fp.inputIdx() != m.anchorStart {
				fp = m.stack.popFrame()
			}
		case urxCaretM: //  ^, test for start of line in mulit-line mode
			if *fp.inputIdx() == m.anchorStart {
				// We are at the start input.  Success.
				break
			}
			// Check whether character just before the current pos is a new-line
			//   unless we are at the end of input
			c := charAt(inputText, *fp.inputIdx()-1)
			if (*fp.inputIdx() < m.anchorLimit) && isLineTerminator(c) {
				//  It's a new-line.  ^ is true.  Success.
				//  TODO:  what should be done with positions between a CR and LF?
				break
			}
			// Not at the start of a line.  Fail.
			fp = m.stack.popFrame()
		case urxCaretMUnix: //  ^, test for start of line in mulit-line + Unix-line mode
			if *fp.inputIdx() <= m.anchorStart {
				// We are at the start input.  Success.
				break
			}

			c := charAt(inputText, *fp.inputIdx()-1)
			if c != 0x0a {
				// Not at the start of a line.  Back-track out.
				fp = m.stack.popFrame()
			}
		case urxBackslashB: // Test for word boundaries
			success := m.isWordBoundary(*fp.inputIdx())
			success = success != (op.value() != 0) // flip sense for \B
			if !success {
				fp = m.stack.popFrame()
			}
		case urxBackslashBu: // Test for word boundaries, Unicode-style
			success := m.isUWordBoundary(*fp.inputIdx())
			success = success != (op.value() != 0) // flip sense for \B
			if !success {
				fp = m.stack.popFrame()
			}
		case urxBackslashD: // Test for decimal digit
			if *fp.inputIdx() >= m.activeLimit {
				m.hitEnd = true
				fp = m.stack.popFrame()
				break
			}

			c := charAt(inputText, *fp.inputIdx())

			success := m.isDecimalDigit(c)
			success = success != (op.value() != 0) // flip sense for \D
			if success {
				*fp.inputIdx()++
			} else {
				fp = m.stack.popFrame()
			}

		case urxBackslashG: // Test for position at end of previous match
			if !((m.match && *fp.inputIdx() == m.matchEnd) || (!m.match && *fp.inputIdx() == m.activeStart)) {
				fp = m.stack.popFrame()
			}

		case urxBackslashH: // Test for \h, horizontal white space.
			if *fp.inputIdx() >= m.activeLimit {
				m.hitEnd = true
				fp = m.stack.popFrame()
				break
			}

			c := charAt(inputText, *fp.inputIdx())
			success := m.isHorizWS(c) || c == 9
			success = success != (op.value() != 0) // flip sense for \H
			if success {
				*fp.inputIdx()++
			} else {
				fp = m.stack.popFrame()
			}

		case urxBackslashR: // Test for \R, any line break sequence.
			if *fp.inputIdx() >= m.activeLimit {
				m.hitEnd = true
				fp = m.stack.popFrame()
				break
			}
			c := charAt(inputText, *fp.inputIdx())
			if isLineTerminator(c) {
				if c == 0x0d && charAt(inputText, *fp.inputIdx()+1) == 0x0a {
					*fp.inputIdx()++
				}
				*fp.inputIdx()++
			} else {
				fp = m.stack.popFrame()
			}

		case urxBackslashV: // \v, any single line ending character.
			if *fp.inputIdx() >= m.activeLimit {
				m.hitEnd = true
				fp = m.stack.popFrame()
				break
			}
			c := charAt(inputText, *fp.inputIdx())
			success := isLineTerminator(c)
			success = success != (op.value() != 0) // flip sense for \V
			if success {
				*fp.inputIdx()++
			} else {
				fp = m.stack.popFrame()
			}

		case urxBackslashX:
			//  Match a Grapheme, as defined by Unicode UAX 29.

			// Fail if at end of input
			if *fp.inputIdx() >= m.activeLimit {
				m.hitEnd = true
				fp = m.stack.popFrame()
				break
			}

			*fp.inputIdx() = m.followingGCBoundary(*fp.inputIdx())
			if *fp.inputIdx() >= m.activeLimit {
				m.hitEnd = true
				*fp.inputIdx() = m.activeLimit
			}

		case urxBackslashZ: // Test for end of Input
			if *fp.inputIdx() < m.anchorLimit {
				fp = m.stack.popFrame()
			} else {
				m.hitEnd = true
				m.requireEnd = true
			}
		case urxStaticSetref:
			// Test input character against one of the predefined sets
			//    (Word Characters, for example)
			// The high bit of the op value is a flag for the match polarity.
			//    0:   success if input char is in set.
			//    1:   success if input char is not in set.
			if *fp.inputIdx() >= m.activeLimit {
				m.hitEnd = true
				fp = m.stack.popFrame()
				break
			}

			success := (op.value() & urxNegSet) == urxNegSet
			negOp := op.value() & ^urxNegSet

			c := charAt(inputText, *fp.inputIdx())
			s := staticPropertySets[negOp]
			if s.ContainsRune(c) {
				success = !success
			}

			if success {
				*fp.inputIdx()++
			} else {
				// the character wasn't in the set.
				fp = m.stack.popFrame()
			}
		case urxStatSetrefN:
			// Test input character for NOT being a member of  one of
			//    the predefined sets (Word Characters, for example)
			if *fp.inputIdx() >= m.activeLimit {
				m.hitEnd = true
				fp = m.stack.popFrame()
				break
			}

			c := charAt(inputText, *fp.inputIdx())
			s := staticPropertySets[op.value()]
			if !s.ContainsRune(c) {
				*fp.inputIdx()++
				break
			}
			// the character wasn't in the set.
			fp = m.stack.popFrame()

		case urxSetref:
			if *fp.inputIdx() >= m.activeLimit {
				m.hitEnd = true
				fp = m.stack.popFrame()
				break
			}

			// There is input left.  Pick up one char and test it for set membership.
			c := charAt(inputText, *fp.inputIdx())

			s := sets[op.value()]
			if s.ContainsRune(c) {
				*fp.inputIdx()++
				break
			}

			// the character wasn't in the set.
			fp = m.stack.popFrame()

		case urxDotany:
			// . matches anything, but stops at end-of-line.
			if *fp.inputIdx() >= m.activeLimit {
				m.hitEnd = true
				fp = m.stack.popFrame()
				break
			}

			c := charAt(inputText, *fp.inputIdx())
			if isLineTerminator(c) {
				// End of line in normal mode.   . does not match.
				fp = m.stack.popFrame()
				break
			}
			*fp.inputIdx()++

		case urxDotanyAll:
			// ., in dot-matches-all (including new lines) mode
			if *fp.inputIdx() >= m.activeLimit {
				// At end of input.  Match failed.  Backtrack out.
				m.hitEnd = true
				fp = m.stack.popFrame()
				break
			}

			c := charAt(inputText, *fp.inputIdx())
			*fp.inputIdx()++
			if c == 0x0d && *fp.inputIdx() < m.activeLimit {
				// In the case of a CR/LF, we need to advance over both.
				nextc := charAt(inputText, *fp.inputIdx())
				if nextc == 0x0a {
					*fp.inputIdx()++
				}
			}

		case urxDotanyUnix:
			// '.' operator, matches all, but stops at end-of-line.
			//   UNIX_LINES mode, so 0x0a is the only recognized line ending.
			if *fp.inputIdx() >= m.activeLimit {
				// At end of input.  Match failed.  Backtrack out.
				m.hitEnd = true
				fp = m.stack.popFrame()
				break
			}

			// There is input left.  Advance over one char, unless we've hit end-of-line
			c := charAt(inputText, *fp.inputIdx())
			if c == 0x0a {
				// End of line in normal mode.   '.' does not match the \n
				fp = m.stack.popFrame()
			} else {
				*fp.inputIdx()++
			}
		case urxJmp:
			*fp.patIdx() = op.value()

		case urxFail:
			isMatch = false
			goto breakFromLoop

		case urxJmpSav:
			fp, err = m.stateSave(*fp.inputIdx(), *fp.patIdx()) // State save to loc following current
			if err != nil {
				return err
			}
			*fp.patIdx() = op.value() // Then JMP.

		case urxJmpSavX:
			// This opcode is used with (x)+, when x can match a zero length string.
			// Same as JMP_SAV, except conditional on the match having made forward progress.
			// Destination of the JMP must be a URX_STO_INP_LOC, from which we get the
			//   data address of the input position at the start of the loop.
			stoOp := pat[op.value()-1]
			frameLoc := stoOp.value()

			prevInputIdx := *fp.extra(frameLoc)
			if prevInputIdx < *fp.inputIdx() {
				// The match did make progress.  Repeat the loop.
				fp, err = m.stateSave(*fp.inputIdx(), *fp.patIdx()) // State save to loc following current
				if err != nil {
					return err
				}
				*fp.patIdx() = op.value() // Then JMP.
				*fp.extra(frameLoc) = *fp.inputIdx()
			}
			// If the input position did not advance, we do nothing here,
			//   execution will fall out of the loop.

		case urxCtrInit:
			*fp.extra(op.value()) = 0 // Set the loop counter variable to zero

			// Pick up the three extra operands that CTR_INIT has, and
			//    skip the pattern location counter past
			instOperandLoc := *fp.patIdx()
			*fp.patIdx() += 3 // Skip over the three operands that CTR_INIT has.

			loopLoc := pat[instOperandLoc].value()
			minCount := int(pat[instOperandLoc+1])
			maxCount := int(pat[instOperandLoc+2])

			if minCount == 0 {
				fp, err = m.stateSave(*fp.inputIdx(), loopLoc+1)
				if err != nil {
					return err
				}
			}
			if maxCount == -1 {
				*fp.extra(op.value() + 1) = *fp.inputIdx() // For loop breaking.
			} else if maxCount == 0 {
				fp = m.stack.popFrame()
			}

		case utxCtrLoop:
			initOp := pat[op.value()]
			opValue := initOp.value()
			pCounter := fp.extra(opValue)
			minCount := int(pat[op.value()+2])
			maxCount := int(pat[op.value()+3])
			*pCounter++
			if *pCounter >= maxCount && maxCount != -1 {
				break
			}

			if *pCounter >= minCount {
				if maxCount == -1 {
					// Loop has no hard upper bound.
					// Check that it is progressing through the input, break if it is not.
					pLastIntputIdx := fp.extra(opValue + 1)
					if *pLastIntputIdx == *fp.inputIdx() {
						break
					}
					*pLastIntputIdx = *fp.inputIdx()
				}
				fp, err = m.stateSave(*fp.inputIdx(), *fp.patIdx())
				if err != nil {
					return err
				}
			} else {
				// Increment time-out counter. (StateSave() does it if count >= minCount)
				m.tickCounter--
				if m.tickCounter <= 0 {
					if err = m.incrementTime(*fp.inputIdx()); err != nil {
						return err
					} // Re-initializes fTickCounter
				}
			}

			*fp.patIdx() = op.value() + 4 // Loop back.

		case urxCtrInitNg:
			*fp.extra(op.value()) = 0 // Set the loop counter variable to zero

			// Pick up the three extra operands that CTR_INIT_NG has, and
			//    skip the pattern location counter past
			instrOperandLoc := *fp.patIdx()
			*fp.patIdx() += 3
			loopLoc := pat[instrOperandLoc].value()
			minCount := pat[instrOperandLoc+1].value()
			maxCount := pat[instrOperandLoc+2].value()

			if maxCount == -1 {
				*fp.extra(op.value() + 1) = *fp.inputIdx() //  Save initial input index for loop breaking.
			}

			if minCount == 0 {
				if maxCount != 0 {
					fp, err = m.stateSave(*fp.inputIdx(), *fp.patIdx())
					if err != nil {
						return err
					}
				}
				*fp.patIdx() = loopLoc + 1
			}

		case urxCtrLoopNg:
			initOp := pat[op.value()]
			pCounter := fp.extra(initOp.value())
			minCount := int(pat[op.value()+2])
			maxCount := int(pat[op.value()+3])
			*pCounter++
			if *pCounter >= maxCount && maxCount != -1 {
				// The loop has matched the maximum permitted number of times.
				//   Break out of here with no action.  Matching will
				//   continue with the following pattern.
				break
			}

			if *pCounter < minCount {
				// We haven't met the minimum number of matches yet.
				//   Loop back for another one.
				*fp.patIdx() = op.value() + 4 // Loop back.
				// Increment time-out counter. (StateSave() does it if count >= minCount)
				m.tickCounter--
				if m.tickCounter <= 0 {
					if err = m.incrementTime(*fp.inputIdx()); err != nil {
						return err
					} // Re-initializes fTickCounter
				}
			} else {
				// We do have the minimum number of matches.

				// If there is no upper bound on the loop iterations, check that the input index
				// is progressing, and stop the loop if it is not.
				if maxCount == -1 {
					lastInputIdx := fp.extra(initOp.value() + 1)
					if *fp.inputIdx() == *lastInputIdx {
						break
					}
					*lastInputIdx = *fp.inputIdx()
				}
			}

			// Loop Continuation: we will fall into the pattern following the loop
			//   (non-greedy, don't execute loop body first), but first do
			//   a state save to the top of the loop, so that a match failure
			//   in the following pattern will try another iteration of the loop.
			fp, err = m.stateSave(*fp.inputIdx(), op.value()+4)
			if err != nil {
				return err
			}

		case urxStoSp:
			m.data[op.value()] = m.stack.len()

		case urxLdSp:
			newStackSize := m.data[op.value()]
			newFp := m.stack.offset(newStackSize)
			if newFp.equals(fp) {
				break
			}
			copy(newFp, fp)
			fp = newFp

			m.stack.setSize(newStackSize)
		case urxBackref:
			groupStartIdx := *fp.extra(op.value())
			groupEndIdx := *fp.extra(op.value() + 1)

			if groupStartIdx < 0 {
				// This capture group has not participated in the match thus far,
				fp = m.stack.popFrame() // FAIL, no match.
				break
			}

			success := true
			for {
				if groupStartIdx >= groupEndIdx {
					success = true
					break
				}

				if *fp.inputIdx() >= m.activeLimit {
					success = false
					m.hitEnd = true
					break
				}

				captureGroupChar := charAt(inputText, groupStartIdx)
				inputChar := charAt(inputText, *fp.inputIdx())
				groupStartIdx++
				*fp.inputIdx()++
				if inputChar != captureGroupChar {
					success = false
					break
				}
			}

			if !success {
				fp = m.stack.popFrame()
			}
		case urxBackrefI:
			groupStartIdx := *fp.extra(op.value())
			groupEndIdx := *fp.extra(op.value() + 1)

			if groupStartIdx < 0 {
				// This capture group has not participated in the match thus far,
				fp = m.stack.popFrame() // FAIL, no match.
				break
			}

			captureGroupItr := newCaseFoldIterator(m.input, groupStartIdx, groupEndIdx)
			inputItr := newCaseFoldIterator(m.input, *fp.inputIdx(), m.activeLimit)
			success := true

			for {
				captureGroupChar := captureGroupItr.next()
				if captureGroupChar == -1 {
					success = true
					break
				}
				inputChar := inputItr.next()
				if inputChar == -1 {
					success = false
					m.hitEnd = true
					break
				}
				if inputChar != captureGroupChar {
					success = false
					break
				}
			}

			if success && inputItr.inExpansion() {
				// We otained a match by consuming part of a string obtained from
				// case-folding a single code point of the input text.
				// This does not count as an overall match.
				success = false
			}

			if success {
				*fp.inputIdx() = inputItr.index
			} else {
				fp = m.stack.popFrame()
			}

		case urxStoInpLoc:
			*fp.extra(op.value()) = *fp.inputIdx()

		case urxJmpx:
			instrOperandLoc := *fp.patIdx()
			*fp.patIdx()++
			dataLoc := pat[instrOperandLoc].value()

			saveInputIdx := *fp.extra(dataLoc)

			if saveInputIdx < *fp.inputIdx() {
				*fp.patIdx() = op.value() // JMP
			} else {
				fp = m.stack.popFrame() // FAIL, no progress in loop.
			}

		case urxLaStart:
			m.data[op.value()] = m.stack.len()
			m.data[op.value()+1] = *fp.inputIdx()
			m.data[op.value()+2] = m.activeStart
			m.data[op.value()+3] = m.activeLimit
			m.activeStart = m.lookStart // Set the match region change for
			m.activeLimit = m.lookLimit //   transparent bounds.

		case urxLaEnd:
			stackSize := m.stack.len()
			newStackSize := m.data[op.value()]
			if stackSize > newStackSize {
				// Copy the current top frame back to the new (cut back) top frame.
				//   This makes the capture groups from within the look-ahead
				//   expression available.
				newFp := m.stack.offset(newStackSize)
				copy(newFp, fp)
				fp = newFp
				m.stack.setSize(newStackSize)
			}

			*fp.inputIdx() = m.data[op.value()+1]

			m.activeStart = m.data[op.value()+2]
			m.activeLimit = m.data[op.value()+3]

		case urcOnecharI:
			// Case insensitive one char.  The char from the pattern is already case folded.
			// Input text is not, but case folding the input can not reduce two or more code
			// points to one.
			if *fp.inputIdx() < m.activeLimit {
				c := charAt(inputText, *fp.inputIdx())
				if ucase.Fold(c) == op.value32() {
					*fp.inputIdx()++
					break
				}
			} else {
				m.hitEnd = true
			}

			fp = m.stack.popFrame()

		case urxStringI:
			// Case-insensitive test input against a literal string.
			// Strings require two slots in the compiled pattern, one for the
			//   offset to the string text, and one for the length.
			//   The compiled string has already been case folded.
			patternString := litText[op.value():]
			var patternStringIdx int
			nextOp := pat[*fp.patIdx()]
			*fp.patIdx()++
			patternStringLen := nextOp.value()

			success := true

			it := newCaseFoldIterator(inputText, *fp.inputIdx(), m.activeLimit)
			for patternStringIdx < patternStringLen {
				cText := it.next()
				cPattern := patternString[patternStringIdx]
				patternStringIdx++

				if cText != cPattern {
					success = false
					if cText == -1 {
						m.hitEnd = true
					}
					break
				}
			}
			if it.inExpansion() {
				success = false
			}

			if success {
				*fp.inputIdx() = it.index
			} else {
				fp = m.stack.popFrame()
			}

		case urxLbStart:
			// Entering a look-behind block.
			// Save Stack Ptr, Input Pos and active input region.
			//   TODO:  implement transparent bounds.  Ticket #6067
			m.data[op.value()] = m.stack.len()
			m.data[op.value()+1] = *fp.inputIdx()
			// Save input string length, then reset to pin any matches to end at
			//   the current position.
			m.data[op.value()+2] = m.activeStart
			m.data[op.value()+3] = m.activeLimit
			m.activeStart = m.regionStart
			m.activeLimit = *fp.inputIdx()
			// Init the variable containing the start index for attempted matches.
			m.data[op.value()+4] = -1
		case urxLbCont:
			// Positive Look-Behind, at top of loop checking for matches of LB expression
			//    at all possible input starting positions.

			// Fetch the min and max possible match lengths.  They are the operands
			//   of this op in the pattern.
			minML := pat[*fp.patIdx()]
			*fp.patIdx()++
			maxML := pat[*fp.patIdx()]
			*fp.patIdx()++

			lbStartIdx := &m.data[op.value()+4]
			if *lbStartIdx < 0 {
				// First time through loop.
				*lbStartIdx = *fp.inputIdx() - int(minML)
				if *lbStartIdx > 0 {
					*lbStartIdx = *fp.inputIdx()
				}
			} else {
				// 2nd through nth time through the loop.
				// Back up start position for match by one.
				*lbStartIdx--
			}

			if *lbStartIdx < 0 || *lbStartIdx < *fp.inputIdx()-int(maxML) {
				// We have tried all potential match starting points without
				//  getting a match.  Backtrack out, and out of the
				//   Look Behind altogether.
				fp = m.stack.popFrame()
				m.activeStart = m.data[op.value()+2]
				m.activeLimit = m.data[op.value()+3]
				break
			}

			//    Save state to this URX_LB_CONT op, so failure to match will repeat the loop.
			//      (successful match will fall off the end of the loop.)
			fp, err = m.stateSave(*fp.inputIdx(), *fp.patIdx()-3)
			if err != nil {
				return err
			}
			*fp.inputIdx() = *lbStartIdx

		case urxLbEnd:
			// End of a look-behind block, after a successful match.
			if *fp.inputIdx() != m.activeLimit {
				//  The look-behind expression matched, but the match did not
				//    extend all the way to the point that we are looking behind from.
				//  FAIL out of here, which will take us back to the LB_CONT, which
				//     will retry the match starting at another position or fail
				//     the look-behind altogether, whichever is appropriate.
				fp = m.stack.popFrame()
				break
			}

			// Look-behind match is good.  Restore the orignal input string region,
			//   which had been truncated to pin the end of the lookbehind match to the
			//   position being looked-behind.
			m.activeStart = m.data[op.value()+2]
			m.activeLimit = m.data[op.value()+3]
		case urxLbnCount:
			// Negative Look-Behind, at top of loop checking for matches of LB expression
			//    at all possible input starting positions.

			// Fetch the extra parameters of this op.
			minML := pat[*fp.patIdx()]
			*fp.patIdx()++
			maxML := pat[*fp.patIdx()]
			*fp.patIdx()++

			continueLoc := pat[*fp.patIdx()].value()
			*fp.patIdx()++

			lbStartIdx := &m.data[op.value()+4]

			if *lbStartIdx < 0 {
				// First time through loop.
				*lbStartIdx = *fp.inputIdx() - int(minML)
				if *lbStartIdx > 0 {
					// move index to a code point boundary, if it's not on one already.
					*lbStartIdx = *fp.inputIdx()
				}
			} else {
				// 2nd through nth time through the loop.
				// Back up start position for match by one.
				*lbStartIdx--
			}

			if *lbStartIdx < 0 || *lbStartIdx < *fp.inputIdx()-int(maxML) {
				// We have tried all potential match starting points without
				//  getting a match, which means that the negative lookbehind as
				//  a whole has succeeded.  Jump forward to the continue location
				m.activeStart = m.data[op.value()+2]
				m.activeLimit = m.data[op.value()+3]
				*fp.patIdx() = continueLoc
				break
			}

			//    Save state to this URX_LB_CONT op, so failure to match will repeat the loop.
			//      (successful match will cause a FAIL out of the loop altogether.)
			fp, err = m.stateSave(*fp.inputIdx(), *fp.patIdx()-4)
			if err != nil {
				return err
			}
			*fp.inputIdx() = *lbStartIdx
		case urxLbnEnd:
			// End of a negative look-behind block, after a successful match.

			if *fp.inputIdx() != m.activeLimit {
				//  The look-behind expression matched, but the match did not
				//    extend all the way to the point that we are looking behind from.
				//  FAIL out of here, which will take us back to the LB_CONT, which
				//     will retry the match starting at another position or succeed
				//     the look-behind altogether, whichever is appropriate.
				fp = m.stack.popFrame()
				break
			}

			// Look-behind expression matched, which means look-behind test as
			//   a whole Fails

			//   Restore the orignal input string length, which had been truncated
			//   inorder to pin the end of the lookbehind match
			//   to the position being looked-behind.
			m.activeStart = m.data[op.value()+2]
			m.activeLimit = m.data[op.value()+3]

			// Restore original stack position, discarding any state saved
			//   by the successful pattern match.
			newStackSize := m.data[op.value()]
			m.stack.setSize(newStackSize)

			//  FAIL, which will take control back to someplace
			//  prior to entering the look-behind test.
			fp = m.stack.popFrame()
		case urxLoopSrI:
			// Loop Initialization for the optimized implementation of
			//     [some character set]*
			//   This op scans through all matching input.
			//   The following LOOP_C op emulates stack unwinding if the following pattern fails.
			s := sets[op.value()]

			// Loop through input, until either the input is exhausted or
			//   we reach a character that is not a member of the set.
			ix := *fp.inputIdx()

			for {
				if ix >= m.activeLimit {
					m.hitEnd = true
					break
				}
				c := charAt(inputText, ix)
				if !s.ContainsRune(c) {
					break
				}
				ix++
			}

			// If there were no matching characters, skip over the loop altogether.
			//   The loop doesn't run at all, a * op always succeeds.
			if ix == *fp.inputIdx() {
				*fp.patIdx()++ // skip the URX_LOOP_C op.
				break
			}

			// Peek ahead in the compiled pattern, to the URX_LOOP_C that
			//   must follow.  It's operand is the stack location
			//   that holds the starting input index for the match of this [set]*
			loopcOp := pat[*fp.patIdx()]
			stackLoc := loopcOp.value()
			*fp.extra(stackLoc) = *fp.inputIdx()
			*fp.inputIdx() = ix

			// Save State to the URX_LOOP_C op that follows this one,
			//   so that match failures in the following code will return to there.
			//   Then bump the pattern idx so the LOOP_C is skipped on the way out of here.
			fp, err = m.stateSave(*fp.inputIdx(), *fp.patIdx())
			if err != nil {
				return err
			}
			*fp.patIdx()++
		case urxLoopDotI:
			// Loop Initialization for the optimized implementation of .*
			//   This op scans through all remaining input.
			//   The following LOOP_C op emulates stack unwinding if the following pattern fails.

			// Loop through input until the input is exhausted (we reach an end-of-line)
			// In DOTALL mode, we can just go straight to the end of the input.
			var ix int
			if (op.value() & 1) == 1 {
				// Dot-matches-All mode.  Jump straight to the end of the string.
				ix = m.activeLimit
				m.hitEnd = true
			} else {
				// NOT DOT ALL mode.  Line endings do not match '.'
				// Scan forward until a line ending or end of input.
				ix = *fp.inputIdx()
				for {
					if ix >= m.activeLimit {
						m.hitEnd = true
						break
					}
					c := charAt(inputText, ix)
					if (c & 0x7f) <= 0x29 { // Fast filter of non-new-line-s
						if (c == 0x0a) || //  0x0a is newline in both modes.
							(((op.value() & 2) == 0) && // IF not UNIX_LINES mode
								isLineTerminator(c)) {
							//  char is a line ending.  Exit the scanning loop.
							break
						}
					}
					ix++
				}
			}

			// If there were no matching characters, skip over the loop altogether.
			//   The loop doesn't run at all, a * op always succeeds.
			if ix == *fp.inputIdx() {
				*fp.patIdx()++ // skip the URX_LOOP_C op.
				break
			}

			// Peek ahead in the compiled pattern, to the URX_LOOP_C that
			//   must follow.  It's operand is the stack location
			//   that holds the starting input index for the match of this .*
			loopcOp := pat[*fp.patIdx()]
			stackLoc := loopcOp.value()
			*fp.extra(stackLoc) = *fp.inputIdx()
			*fp.inputIdx() = ix

			// Save State to the URX_LOOP_C op that follows this one,
			//   so that match failures in the following code will return to there.
			//   Then bump the pattern idx so the LOOP_C is skipped on the way out of here.
			fp, err = m.stateSave(*fp.inputIdx(), *fp.patIdx())
			if err != nil {
				return err
			}
			*fp.patIdx()++

		case urxLoopC:
			backSearchIndex := *fp.extra(op.value())

			if backSearchIndex == *fp.inputIdx() {
				// We've backed up the input idx to the point that the loop started.
				// The loop is done.  Leave here without saving state.
				//  Subsequent failures won't come back here.
				break
			}
			// Set up for the next iteration of the loop, with input index
			//   backed up by one from the last time through,
			//   and a state save to this instruction in case the following code fails again.
			//   (We're going backwards because this loop emulates stack unwinding, not
			//    the initial scan forward.)

			prevC := charAt(inputText, *fp.inputIdx()-1)
			*fp.inputIdx()--
			twoPrevC := charAt(inputText, *fp.inputIdx()-1)

			if prevC == 0x0a &&
				*fp.inputIdx() > backSearchIndex &&
				twoPrevC == 0x0d {
				prevOp := pat[*fp.patIdx()-2]
				if prevOp.typ() == urxLoopDotI {
					// .*, stepping back over CRLF pair.
					*fp.inputIdx()--
				}
			}

			fp, err = m.stateSave(*fp.inputIdx(), *fp.patIdx()-1)
			if err != nil {
				return err
			}
		default:
			// Trouble.  The compiled pattern contains an entry with an
			//           unrecognized type tag.
			panic("unreachable")
		}
	}

breakFromLoop:
	m.match = isMatch
	if isMatch {
		m.lastMatchEnd = m.matchEnd
		m.matchStart = startIdx
		m.matchEnd = *fp.inputIdx()
	}

	if m.dumper != nil {
		if isMatch {
			fmt.Fprintf(m.dumper, "Match.  start=%d   end=%d\n\n", m.matchStart, m.matchEnd)
		} else {
			fmt.Fprintf(m.dumper, "No match\n\n")
		}
	}

	m.frame = fp // The active stack frame when the engine stopped.
	//   Contains the capture group results that we need to
	//    access later.
	return nil
}

func charAt(str []rune, idx int) rune {
	if idx >= 0 && idx < len(str) {
		return str[idx]
	}
	return -1
}

func (m *Matcher) isWordBoundary(pos int) bool {
	cIsWord := false

	if pos >= m.lookLimit {
		m.hitEnd = true
	} else {
		c := charAt(m.input, pos)
		if uprops.HasBinaryProperty(c, uprops.UCharGraphemeExtend) || uchar.CharType(c) == uchar.FormatChar {
			return false
		}
		cIsWord = staticPropertySets[urxIswordSet].ContainsRune(c)
	}

	prevCIsWord := false
	for {
		if pos <= m.lookStart {
			break
		}
		prevChar := charAt(m.input, pos-1)
		pos--
		if !(uprops.HasBinaryProperty(prevChar, uprops.UCharGraphemeExtend) || uchar.CharType(prevChar) == uchar.FormatChar) {
			prevCIsWord = staticPropertySets[urxIswordSet].ContainsRune(prevChar)
			break
		}
	}
	return cIsWord != prevCIsWord
}

func (m *Matcher) isUWordBoundary(pos int) bool {
	// TODO: implement
	/*
		    UBool       returnVal = FALSE;

		#if UCONFIG_NO_BREAK_ITERATION==0
		    // Note: this point will never be reached if break iteration is configured out.
		    //       Regex patterns that would require this function will fail to compile.

		    // If we haven't yet created a break iterator for this matcher, do it now.
		    if (fWordBreakItr == nullptr) {
		        fWordBreakItr = BreakIterator::createWordInstance(Locale::getEnglish(), status);
		        if (U_FAILURE(status)) {
		            return FALSE;
		        }
		        fWordBreakItr->setText(fInputText, status);
		    }

		    // Note: zero width boundary tests like \b see through transparent region bounds,
		    //       which is why fLookLimit is used here, rather than fActiveLimit.
		    if (pos >= fLookLimit) {
		        fHitEnd = TRUE;
		        returnVal = TRUE;   // With Unicode word rules, only positions within the interior of "real"
		                            //    words are not boundaries.  All non-word chars stand by themselves,
		                            //    with word boundaries on both sides.
		    } else {
		        returnVal = fWordBreakItr->isBoundary((int32_t)pos);
		    }
		#endif
		    return   returnVal;
	*/
	return false
}

func (m *Matcher) resetStack() stackFrame {
	m.stack.reset()
	frame, _ := m.stack.newFrame(0, nil, "")
	frame.clearExtra()
	return frame
}

func (m *Matcher) stateSave(inputIdx, savePatIdx int) (stackFrame, error) {
	// push storage for a new frame.
	newFP, err := m.stack.newFrame(inputIdx, m.input, m.pattern.pattern)
	if err != nil {
		return nil, err
	}
	fp := m.stack.prevFromTop()

	// New stack frame = copy of old top frame.
	copy(newFP, fp)

	m.tickCounter--
	if m.tickCounter <= 0 {
		if err := m.incrementTime(*fp.inputIdx()); err != nil {
			return nil, err
		}
	}
	*fp.patIdx() = savePatIdx
	return newFP, nil
}

func (m *Matcher) incrementTime(inputIdx int) error {
	m.tickCounter = timerInitialValue
	m.time++
	if m.timeLimit > 0 && m.time >= m.timeLimit {
		return &MatchError{
			Code:     TimeOut,
			Pattern:  m.pattern.pattern,
			Position: inputIdx,
			Input:    m.input,
		}
	}
	return nil
}

func (m *Matcher) isDecimalDigit(c rune) bool {
	return uchar.IsDigit(c)
}

func (m *Matcher) isHorizWS(c rune) bool {
	return uchar.CharType(c) == uchar.SpaceSeparator || c == 9
}

func (m *Matcher) followingGCBoundary(pos int) int {
	// TODO: implement
	return pos
	/*
		// Note: this point will never be reached if break iteration is configured out.
		//       Regex patterns that would require this function will fail to compile.

		// If we haven't yet created a break iterator for this matcher, do it now.
		if (m.gcBreakItr == nil) {
			m.gcBreakItr = BreakIterator::createCharacterInstance(Locale::getEnglish(), status);
			if (U_FAILURE(status)) {
				return pos;
			}
			fGCBreakItr->setText(fInputText, status);
		}
		result = fGCBreakItr->following(pos);
		if (result == BreakIterator::DONE) {
			result = pos;
		}
	*/
}

func (m *Matcher) ResetString(input string) {
	m.Reset([]rune(input))
}

func (m *Matcher) Reset(input []rune) {
	m.input = input
	m.reset()
}

func (m *Matcher) Matches() (bool, error) {
	err := m.MatchAt(m.activeStart, true)
	return m.match, err
}

func (m *Matcher) LookingAt() (bool, error) {
	err := m.MatchAt(m.activeStart, false)
	return m.match, err
}

func (m *Matcher) Find() (bool, error) {
	startPos := m.matchEnd
	if startPos == 0 {
		startPos = m.activeStart
	}

	if m.match {
		// Save the position of any previous successful match.
		m.lastMatchEnd = m.matchEnd
		if m.matchStart == m.matchEnd {
			// Previous match had zero length.  Move start position up one position
			//  to avoid sending find() into a loop on zero-length matches.
			if startPos >= m.activeLimit {
				m.match = false
				m.hitEnd = true
				return false, nil
			}
			startPos++
		}
	} else {
		if m.lastMatchEnd >= 0 {
			// A previous find() failed to match.  Don't try again.
			//   (without this test, a pattern with a zero-length match
			//    could match again at the end of an input string.)
			m.hitEnd = true
			return false, nil
		}
	}

	testStartLimit := m.activeLimit - int(m.pattern.minMatchLen)
	if startPos > testStartLimit {
		m.match = false
		m.hitEnd = true
		return false, nil
	}

	switch m.pattern.startType {
	case startNoInfo:
		// No optimization was found.
		//  Try a match at each input position.
		for {
			err := m.MatchAt(startPos, false)
			if err != nil {
				return false, err
			}
			if m.match {
				return true, nil
			}
			if startPos >= testStartLimit {
				m.hitEnd = true
				return false, nil
			}
			startPos++
		}
	case startSet:
		// Match may start on any char from a pre-computed set.
		for {
			pos := startPos
			c := charAt(m.input, startPos)
			startPos++
			// c will be -1 (U_SENTINEL) at end of text, in which case we
			// skip this next block (so we don't have a negative array index)
			// and handle end of text in the following block.
			if c >= 0 && m.pattern.initialChars.ContainsRune(c) {
				err := m.MatchAt(pos, false)
				if err != nil {
					return false, err
				}
				if m.match {
					return true, nil
				}
			}

			if startPos > testStartLimit {
				m.match = false
				m.hitEnd = true
				return false, nil
			}
		}
	case startStart:
		// Matches are only possible at the start of the input string
		//   (pattern begins with ^ or \A)
		if startPos > m.activeStart {
			m.match = false
			return false, nil
		}
		err := m.MatchAt(startPos, false)
		return m.match, err
	case startLine:
		var ch rune
		if startPos == m.anchorStart {
			err := m.MatchAt(startPos, false)
			if err != nil {
				return false, err
			}
			if m.match {
				return true, nil
			}
			ch = charAt(m.input, startPos)
			startPos++
		} else {
			ch = charAt(m.input, startPos-1)
		}

		if m.pattern.flags&UnixLines != 0 {
			for {
				if ch == 0x0a {
					err := m.MatchAt(startPos, false)
					if err != nil {
						return false, err
					}
					if m.match {
						return true, nil
					}
				}
				if startPos >= testStartLimit {
					m.match = false
					m.hitEnd = true
					return false, nil
				}
				ch = charAt(m.input, startPos)
				startPos++
			}
		} else {
			for {
				if isLineTerminator(ch) {
					if ch == 0x0d && startPos < m.activeLimit && charAt(m.input, startPos) == 0x0a {
						startPos++
					}
					err := m.MatchAt(startPos, false)
					if err != nil {
						return false, err
					}
					if m.match {
						return true, nil
					}
				}
				if startPos >= testStartLimit {
					m.match = false
					m.hitEnd = true
					return false, nil
				}
				ch = charAt(m.input, startPos)
				startPos++
			}
		}
	case startChar, startString:
		// Match starts on exactly one char.
		theChar := m.pattern.initialChar
		for {
			pos := startPos
			c := charAt(m.input, startPos)
			startPos++
			if c == theChar {
				err := m.MatchAt(pos, false)
				if err != nil {
					return false, err
				}
				if m.match {
					return true, nil
				}
			}
			if startPos > testStartLimit {
				m.match = false
				m.hitEnd = true
				return false, nil
			}
		}
	default:
		panic("unreachable")
	}
}

func (m *Matcher) Start() int {
	if !m.match {
		return -1
	}

	return m.matchStart
}

func (m *Matcher) reset() {
	m.regionStart = 0
	m.regionLimit = len(m.input)
	m.activeStart = 0
	m.activeLimit = len(m.input)
	m.anchorStart = 0
	m.anchorLimit = len(m.input)
	m.lookStart = 0
	m.lookLimit = len(m.input)
	m.resetPreserveRegion()
}

func (m *Matcher) resetPreserveRegion() {
	m.matchStart = 0
	m.matchEnd = 0
	m.lastMatchEnd = -1
	m.appendPosition = 0
	m.match = false
	m.hitEnd = false
	m.requireEnd = false
	m.time = 0
	m.tickCounter = timerInitialValue
}

func (m *Matcher) GroupCount() int {
	return len(m.pattern.groupMap)
}

func (m *Matcher) StartForGroup(group int) int {
	if !m.match {
		return -1
	}
	if group < 0 || group > len(m.pattern.groupMap) {
		return -1
	}
	if group == 0 {
		return m.matchStart
	}
	groupOffset := int(m.pattern.groupMap[group-1])
	return *m.frame.extra(groupOffset)
}

func (m *Matcher) EndForGroup(group int) int {
	if !m.match {
		return -1
	}
	if group < 0 || group > len(m.pattern.groupMap) {
		return -1
	}
	if group == 0 {
		return m.matchEnd
	}
	groupOffset := int(m.pattern.groupMap[group-1])
	return *m.frame.extra(groupOffset + 1)
}

func (m *Matcher) HitEnd() bool {
	return m.hitEnd
}

func (m *Matcher) RequireEnd() bool {
	return m.requireEnd
}

func (m *Matcher) Group(i int) (string, bool) {
	start := m.StartForGroup(i)
	end := m.EndForGroup(i)
	if start == -1 || end == -1 {
		return "", false
	}
	return string(m.input[start:end]), true
}

func (m *Matcher) End() int {
	if !m.match {
		return -1
	}

	return m.matchEnd
}

func (m *Matcher) Dumper(out io.Writer) {
	m.dumper = out
}

// Test for any of the Unicode line terminating characters.
func isLineTerminator(c rune) bool {
	if (c & ^(0x0a | 0x0b | 0x0c | 0x0d | 0x85 | 0x2028 | 0x2029)) != 0 {
		return false
	}
	return (c <= 0x0d && c >= 0x0a) || c == 0x85 || c == 0x2028 || c == 0x2029
}

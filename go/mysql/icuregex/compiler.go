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
	"math"
	"strings"
	"unicode/utf8"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/mysql/icuregex/internal/pattern"
	"vitess.io/vitess/go/mysql/icuregex/internal/ucase"
	"vitess.io/vitess/go/mysql/icuregex/internal/uchar"
	"vitess.io/vitess/go/mysql/icuregex/internal/unames"
	"vitess.io/vitess/go/mysql/icuregex/internal/uprops"
	"vitess.io/vitess/go/mysql/icuregex/internal/uset"
	"vitess.io/vitess/go/mysql/icuregex/internal/utf16"
)

const BreakIteration = false
const stackSize = 100

type reChar struct {
	char   rune
	quoted bool
}

const (
	parenPlain        = -1
	parenCapturing    = -2
	parenAtomic       = -3
	parenLookahead    = -4
	parenNegLookahead = -5
	parenFlags        = -6
	parenLookBehind   = -7
	parenLookBehindN  = -8
)

type setOperation uint32

const (
	setStart         setOperation = 0<<16 | 1
	setEnd           setOperation = 1<<16 | 2
	setNegation      setOperation = 2<<16 | 3
	setCaseClose     setOperation = 2<<16 | 9
	setDifference2   setOperation = 3<<16 | 4 // '--' set difference operator
	setIntersection2 setOperation = 3<<16 | 5 // '&&' set intersection operator
	setUnion         setOperation = 4<<16 | 6 // implicit union of adjacent items
	setDifference1   setOperation = 4<<16 | 7 // '-', single dash difference op, for compatibility with old UnicodeSet.
	setIntersection1 setOperation = 4<<16 | 8 // '&', single amp intersection op, for compatibility with old UnicodeSet.
)

type compiler struct {
	err error
	out *Pattern
	p   []rune

	scanIndex        int
	quoteMode        bool
	inBackslashQuote bool
	eolComments      bool

	lineNum  int
	charNum  int
	lastChar rune
	peekChar rune

	c        reChar
	stack    [stackSize]uint16
	stackPtr int

	modeFlags    RegexpFlag
	newModeFlags RegexpFlag
	setModeFlag  bool

	literalChars []rune

	parenStack      []int
	matchOpenParen  int
	matchCloseParen int

	intervalLow   int
	intervalUpper int

	setStack   []*uset.UnicodeSet
	setOpStack []setOperation

	lastSetLiteral rune
	captureName    *strings.Builder
}

func newCompiler(pat *Pattern) *compiler {
	return &compiler{
		out:             pat,
		scanIndex:       0,
		eolComments:     true,
		lineNum:         1,
		charNum:         0,
		lastChar:        -1,
		peekChar:        -1,
		modeFlags:       RegexpFlag(uint32(pat.flags) | 0x80000000),
		matchOpenParen:  -1,
		matchCloseParen: -1,
		lastSetLiteral:  -1,
	}
}

func (c *compiler) nextCharLL() (ch rune) {
	if c.peekChar != -1 {
		ch, c.peekChar = c.peekChar, -1
		return
	}
	if len(c.p) == 0 {
		return -1
	}

	ch = c.p[0]
	c.p = c.p[1:]
	if ch == utf8.RuneError {
		return -1
	}

	if ch == chCR || ch == chNEL || ch == chLS || (ch == chLF && c.lastChar != chCR) {
		c.lineNum++
		c.charNum = 0
	} else {
		if ch != chLF {
			c.charNum++
		}
	}
	c.lastChar = ch
	return
}

func (c *compiler) peekCharLL() rune {
	if c.peekChar == -1 {
		c.peekChar = c.nextCharLL()
	}
	return c.peekChar
}

func (c *compiler) nextChar(ch *reChar) {
	c.scanIndex++
	ch.char = c.nextCharLL()
	ch.quoted = false

	if c.quoteMode {
		ch.quoted = true
		if (ch.char == chBackSlash && c.peekCharLL() == chE && ((c.modeFlags & Literal) == 0)) ||
			ch.char == -1 {
			c.quoteMode = false //  Exit quote mode,
			c.nextCharLL()      // discard the E
			c.nextChar(ch)
			return
		}
	} else if c.inBackslashQuote {
		// The current character immediately follows a '\'
		// Don't check for any further escapes, just return it as-is.
		// Don't set c.fQuoted, because that would prevent the state machine from
		//    dispatching on the character.
		c.inBackslashQuote = false
	} else {
		// We are not in a \Q quoted region \E of the source.
		//
		if (c.modeFlags & Comments) != 0 {
			//
			// We are in free-spacing and comments mode.
			//  Scan through any white space and comments, until we
			//  reach a significant character or the end of inut.
			for {
				if ch.char == -1 {
					break // End of Input
				}
				if ch.char == chPound && c.eolComments {
					// Start of a comment.  Consume the rest of it, until EOF or a new line
					for {
						ch.char = c.nextCharLL()
						if ch.char == -1 || // EOF
							ch.char == chCR ||
							ch.char == chLF ||
							ch.char == chNEL ||
							ch.char == chLS {
							break
						}
					}
				}
				// TODO:  check what Java & Perl do with non-ASCII white spaces.  Ticket 6061.
				if !pattern.IsWhitespace(ch.char) {
					break
				}
				ch.char = c.nextCharLL()
			}
		}

		//
		//  check for backslash escaped characters.
		//
		if ch.char == chBackSlash {
			beforeEscape := c.p
			if staticSetUnescape.ContainsRune(c.peekCharLL()) {
				//
				// A '\' sequence that is handled by ICU's standard unescapeAt function.
				//   Includes \uxxxx, \n, \r, many others.
				//   Return the single equivalent character.
				//
				c.nextCharLL() // get & discard the peeked char.
				ch.quoted = true

				ch.char, c.p = pattern.UnescapeAtRunes(beforeEscape)
				if ch.char < 0 {
					c.error(BadEscapeSequence)
				}
				c.charNum += len(beforeEscape) - len(c.p)
			} else if c.peekCharLL() == chDigit0 {
				//  Octal Escape, using Java Regexp Conventions
				//    which are \0 followed by 1-3 octal digits.
				//    Different from ICU Unescape handling of Octal, which does not
				//    require the leading 0.
				//  Java also has the convention of only consuming 2 octal digits if
				//    the three digit number would be > 0xff
				//
				ch.char = 0
				c.nextCharLL() // Consume the initial 0.
				for index := 0; index < 3; index++ {
					ch2 := c.peekCharLL()
					if ch2 < chDigit0 || ch2 > chDigit7 {
						if index == 0 {
							// \0 is not followed by any octal digits.
							c.error(BadEscapeSequence)
						}
						break
					}
					ch.char <<= 3
					ch.char += ch2 & 7
					if ch.char <= 255 {
						c.nextCharLL()
					} else {
						// The last digit made the number too big.  Forget we saw it.
						ch.char >>= 3
					}
				}
				ch.quoted = true
			} else if c.peekCharLL() == chQ {
				//  "\Q"  enter quote mode, which will continue until "\E"
				c.quoteMode = true
				c.nextCharLL() // discard the 'Q'.
				c.nextChar(ch) // recurse to get the real next char.
				return
			} else {
				// We are in a '\' escape that will be handled by the state table scanner.
				// Just return the backslash, but remember that the following char is to
				//  be taken literally.
				c.inBackslashQuote = true
			}
		}
	}

	// re-enable # to end-of-line comments, in case they were disabled.
	// They are disabled by the parser upon seeing '(?', but this lasts for
	//  the fetching of the next character only.
	c.eolComments = true
}

const (
	chCR        = 0x0d // New lines, for terminating comments.
	chLF        = 0x0a // Line Feed
	chPound     = 0x23 // '#', introduces a comment.
	chDigit0    = 0x30 // '0'
	chDigit7    = 0x37 // '9'
	chColon     = 0x3A // ':'
	chE         = 0x45 // 'E'
	chQ         = 0x51 // 'Q'
	chN         = 0x4E // 'N'
	chP         = 0x50 // 'P'
	chBackSlash = 0x5c // '\'  introduces a char escape
	chLBracket  = 0x5b // '['
	chRBracket  = 0x5d // ']'
	chUp        = 0x5e // '^'
	chLowerP    = 0x70
	chLBrace    = 0x7b   // '{'
	chRBrace    = 0x7d   // '}'
	chNEL       = 0x85   //    NEL newline variant
	chLS        = 0x2028 //    Unicode Line Separator
	chAmp       = 0x26   // '&'
	chDash      = 0x2d   // '-'
)

func (c *compiler) compile(pat []rune) error {
	if c.err != nil {
		return c.err
	}
	if c.out.pattern != "" {
		panic("cannot reuse pattern")
	}

	c.out.pattern = string(pat)
	c.p = pat

	var state uint16 = 1
	var table []regexTableEl

	// UREGEX_LITERAL force entire pattern to be treated as a literal string.
	if c.modeFlags&Literal != 0 {
		c.quoteMode = true
	}

	c.nextChar(&c.c)

	// Main loop for the regex pattern parsing state machine.
	//   Runs once per state transition.
	//   Each time through optionally performs, depending on the state table,
	//      - an advance to the the next pattern char
	//      - an action to be performed.
	//      - pushing or popping a state to/from the local state return stack.
	//   file regexcst.txt is the source for the state table.  The logic behind
	//     recongizing the pattern syntax is there, not here.
	for {
		if c.err != nil {
			break
		}

		if state == 0 {
			panic("bad state?")
		}

		table = parseStateTable[state:]
		for len(table) > 0 {
			if table[0].charClass < 127 && !c.c.quoted && rune(table[0].charClass) == c.c.char {
				break
			}
			if table[0].charClass == 255 {
				break
			}
			if table[0].charClass == 254 && c.c.quoted {
				break
			}
			if table[0].charClass == 253 && c.c.char == -1 {
				break
			}
			if table[0].charClass >= 128 && table[0].charClass < 240 && !c.c.quoted && c.c.char != -1 {
				if staticRuleSet[table[0].charClass-128].ContainsRune(c.c.char) {
					break
				}
			}

			table = table[1:]
		}

		if !c.doParseActions(table[0].action) {
			break
		}

		if table[0].pushState != 0 {
			c.stackPtr++
			if c.stackPtr >= stackSize {
				c.error(InternalError)
				c.stackPtr--
			}
			c.stack[c.stackPtr] = uint16(table[0].pushState)
		}

		if table[0].nextChar {
			c.nextChar(&c.c)
		}

		if table[0].nextState != 255 {
			state = uint16(table[0].nextState)
		} else {
			state = c.stack[c.stackPtr]
			c.stackPtr--
			if c.stackPtr < 0 {
				c.stackPtr++
				c.error(MismatchedParen)
			}
		}
	}

	if c.err != nil {
		return c.err
	}

	c.allocateStackData(restackframeHdrCount)
	c.stripNOPs()

	c.out.minMatchLen = c.minMatchLength(3, len(c.out.compiledPat)-1)

	c.matchStartType()
	return c.err
}

func (c *compiler) doParseActions(action patternParseAction) bool {
	switch action {
	case doPatStart:
		// Start of pattern compiles to:
		//0   SAVE   2        Fall back to position of FAIL
		//1   jmp    3
		//2   FAIL            Stop if we ever reach here.
		//3   NOP             Dummy, so start of pattern looks the same as
		//                    the start of an ( grouping.
		//4   NOP             Resreved, will be replaced by a save if there are
		//                    OR | operators at the top level
		c.appendOp(urxStateSave, 2)
		c.appendOp(urxJmp, 3)
		c.appendOp(urxFail, 0)

		// Standard open nonCapture paren action emits the two NOPs and
		//   sets up the paren stack frame.
		c.doParseActions(doOpenNonCaptureParen)

	case doPatFinish:
		// We've scanned to the end of the pattern
		//  The end of pattern compiles to:
		//        URX_END
		//    which will stop the runtime match engine.
		//  Encountering end of pattern also behaves like a close paren,
		//   and forces fixups of the State Save at the beginning of the compiled pattern
		//   and of any OR operations at the top level.
		//
		c.handleCloseParen()
		if len(c.parenStack) > 0 {
			// Missing close paren in pattern.
			c.error(MismatchedParen)
		}

		// add the END operation to the compiled pattern.
		c.appendOp(urxEnd, 0)

		// Terminate the pattern compilation state machine.
		return false

	case doOrOperator:
		// Scanning a '|', as in (A|B)
		// Generate code for any pending literals preceding the '|'
		c.fixLiterals(false)

		// Insert a SAVE operation at the start of the pattern section preceding
		//   this OR at this level.  This SAVE will branch the match forward
		//   to the right hand side of the OR in the event that the left hand
		//   side fails to match and backtracks.  Locate the position for the
		//   save from the location on the top of the parentheses stack.
		var savePosition int
		savePosition, c.parenStack = stackPop(c.parenStack)
		op := c.out.compiledPat[savePosition]

		if op.typ() != urxNop {
			panic("expected a NOP placeholder")
		}

		op = c.buildOp(urxStateSave, len(c.out.compiledPat)+1)
		c.out.compiledPat[savePosition] = op

		// Append an JMP operation into the compiled pattern.  The operand for
		//  the JMP will eventually be the location following the ')' for the
		//  group.  This will be patched in later, when the ')' is encountered.
		c.appendOp(urxJmp, 0)

		// Push the position of the newly added JMP op onto the parentheses stack.
		// This registers if for fixup when this block's close paren is encountered.
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-1)

		// Append a NOP to the compiled pattern.  This is the slot reserved
		//   for a SAVE in the event that there is yet another '|' following
		//   this one.
		c.appendOp(urxNop, 0)
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-1)

	case doBeginNamedCapture:
		// Scanning (?<letter.
		//   The first letter of the name will come through again under doConinueNamedCapture.
		c.captureName = &strings.Builder{}

	case doContinueNamedCapture:
		c.captureName.WriteRune(c.c.char)

	case doBadNamedCapture:
		c.error(InvalidCaptureGroupName)

	case doOpenCaptureParen:
		// Open Capturing Paren, possibly named.
		//   Compile to a
		//      - NOP, which later may be replaced by a save-state if the
		//         parenthesized group gets a * quantifier, followed by
		//      - START_CAPTURE  n    where n is stack frame offset to the capture group variables.
		//      - NOP, which may later be replaced by a save-state if there
		//             is an '|' alternation within the parens.
		//
		//    Each capture group gets three slots in the save stack frame:
		//         0: Capture Group start position (in input string being matched.)
		//         1: Capture Group end position.
		//         2: Start of Match-in-progress.
		//    The first two locations are for a completed capture group, and are
		//     referred to by back references and the like.
		//    The third location stores the capture start position when an START_CAPTURE is
		//      encountered.  This will be promoted to a completed capture when (and if) the corresponding
		//      END_CAPTURE is encountered.
		c.fixLiterals(false)
		c.appendOp(urxNop, 0)
		varsLoc := c.allocateStackData(3) // Reserve three slots in match stack frame.
		c.appendOp(urxStartCapture, varsLoc)
		c.appendOp(urxNop, 0)

		// On the Parentheses stack, start a new frame and add the postions
		//   of the two NOPs.  Depending on what follows in the pattern, the
		//   NOPs may be changed to SAVE_STATE or JMP ops, with a target
		//   address of the end of the parenthesized group.
		c.parenStack = append(c.parenStack, int(c.modeFlags))         // Match mode state
		c.parenStack = append(c.parenStack, parenCapturing)           // Frame type.
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-3) // The first  NOP location
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-1) // The second NOP loc

		// Save the mapping from group number to stack frame variable position.
		c.out.groupMap = append(c.out.groupMap, int32(varsLoc))

		if c.captureName != nil {
			if c.out.namedCaptureMap == nil {
				c.out.namedCaptureMap = make(map[string]int)
			}

			groupNumber := len(c.out.groupMap)
			captureName := c.captureName.String()
			c.captureName = nil

			if _, ok := c.out.namedCaptureMap[captureName]; ok {
				c.error(InvalidCaptureGroupName)
			}
			c.out.namedCaptureMap[captureName] = groupNumber
		}

	case doOpenNonCaptureParen:
		// Open non-caputuring (grouping only) Paren.
		//   Compile to a
		//      - NOP, which later may be replaced by a save-state if the
		//         parenthesized group gets a * quantifier, followed by
		//      - NOP, which may later be replaced by a save-state if there
		//             is an '|' alternation within the parens.
		c.fixLiterals(false)
		c.appendOp(urxNop, 0)
		c.appendOp(urxNop, 0)

		// On the Parentheses stack, start a new frame and add the postions
		//   of the two NOPs.
		c.parenStack = append(c.parenStack, int(c.modeFlags))
		c.parenStack = append(c.parenStack, parenPlain)
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-2)
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-1)

	case doOpenAtomicParen:
		// Open Atomic Paren.  (?>
		//   Compile to a
		//      - NOP, which later may be replaced if the parenthesized group
		//         has a quantifier, followed by
		//      - STO_SP  save state stack position, so it can be restored at the ")"
		//      - NOP, which may later be replaced by a save-state if there
		//             is an '|' alternation within the parens.
		c.fixLiterals(false)
		c.appendOp(urxNop, 0)
		varLoc := c.allocateData(1) // Reserve a data location for saving the state stack ptr.
		c.appendOp(urxStoSp, varLoc)
		c.appendOp(urxNop, 0)

		// On the Parentheses stack, start a new frame and add the postions
		//   of the two NOPs.  Depending on what follows in the pattern, the
		//   NOPs may be changed to SAVE_STATE or JMP ops, with a target
		//   address of the end of the parenthesized group.
		c.parenStack = append(c.parenStack, int(c.modeFlags))
		c.parenStack = append(c.parenStack, parenAtomic)
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-3)
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-1)

	case doOpenLookAhead:
		// Positive Look-ahead   (?=  stuff  )
		//
		//   Note:   Addition of transparent input regions, with the need to
		//           restore the original regions when failing out of a lookahead
		//           block, complicated this sequence.  Some combined opcodes
		//           might make sense - or might not, lookahead aren't that common.
		//
		//      Caution:  min match length optimization knows about this
		//               sequence; don't change without making updates there too.
		//
		// Compiles to
		//    1    LA_START     dataLoc     Saves SP, Input Pos, Active input region.
		//    2.   STATE_SAVE   4            on failure of lookahead, goto 4
		//    3    JMP          6           continue ...
		//
		//    4.   LA_END                   Look Ahead failed.  Restore regions.
		//    5.   BACKTRACK                and back track again.
		//
		//    6.   NOP              reserved for use by quantifiers on the block.
		//                          Look-ahead can't have quantifiers, but paren stack
		//                             compile time conventions require the slot anyhow.
		//    7.   NOP              may be replaced if there is are '|' ops in the block.
		//    8.     code for parenthesized stuff.
		//    9.   LA_END
		//
		//  Four data slots are reserved, for saving state on entry to the look-around
		//    0:   stack pointer on entry.
		//    1:   input position on entry.
		//    2:   fActiveStart, the active bounds start on entry.
		//    3:   fActiveLimit, the active bounds limit on entry.
		c.fixLiterals(false)
		dataLoc := c.allocateData(4)
		c.appendOp(urxLaStart, dataLoc)
		c.appendOp(urxStateSave, len(c.out.compiledPat)+2)
		c.appendOp(urxJmp, len(c.out.compiledPat)+3)
		c.appendOp(urxLaEnd, dataLoc)
		c.appendOp(urxBacktrack, 0)
		c.appendOp(urxNop, 0)
		c.appendOp(urxNop, 0)

		// On the Parentheses stack, start a new frame and add the postions
		//   of the NOPs.
		c.parenStack = append(c.parenStack, int(c.modeFlags))
		c.parenStack = append(c.parenStack, parenLookahead)
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-2)
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-1)

	case doOpenLookAheadNeg:
		// Negated Lookahead.   (?! stuff )
		// Compiles to
		//    1.    LA_START    dataloc
		//    2.    SAVE_STATE  7         // Fail within look-ahead block restores to this state,
		//                                //   which continues with the match.
		//    3.    NOP                   // Std. Open Paren sequence, for possible '|'
		//    4.       code for parenthesized stuff.
		//    5.    LA_END                // Cut back stack, remove saved state from step 2.
		//    6.    BACKTRACK             // code in block succeeded, so neg. lookahead fails.
		//    7.    END_LA                // Restore match region, in case look-ahead was using
		//                                        an alternate (transparent) region.
		//  Four data slots are reserved, for saving state on entry to the look-around
		//    0:   stack pointer on entry.
		//    1:   input position on entry.
		//    2:   fActiveStart, the active bounds start on entry.
		//    3:   fActiveLimit, the active bounds limit on entry.
		c.fixLiterals(false)
		dataLoc := c.allocateData(4)
		c.appendOp(urxLaStart, dataLoc)
		c.appendOp(urxStateSave, 0) // dest address will be patched later.
		c.appendOp(urxNop, 0)

		// On the Parentheses stack, start a new frame and add the postions
		//   of the StateSave and NOP.
		c.parenStack = append(c.parenStack, int(c.modeFlags))
		c.parenStack = append(c.parenStack, parenNegLookahead)
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-2)
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-1)

		// Instructions #5 - #7 will be added when the ')' is encountered.

	case doOpenLookBehind:
		//   Compile a (?<= look-behind open paren.
		//
		//          Compiles to
		//              0       URX_LB_START     dataLoc
		//              1       URX_LB_CONT      dataLoc
		//              2                        MinMatchLen
		//              3                        MaxMatchLen
		//              4       URX_NOP          Standard '(' boilerplate.
		//              5       URX_NOP          Reserved slot for use with '|' ops within (block).
		//              6         <code for LookBehind expression>
		//              7       URX_LB_END       dataLoc    # Check match len, restore input  len
		//              8       URX_LA_END       dataLoc    # Restore stack, input pos
		//
		//          Allocate a block of matcher data, to contain (when running a match)
		//              0:    Stack ptr on entry
		//              1:    Input Index on entry
		//              2:    fActiveStart, the active bounds start on entry.
		//              3:    fActiveLimit, the active bounds limit on entry.
		//              4:    Start index of match current match attempt.
		//          The first four items must match the layout of data for LA_START / LA_END

		// Generate match code for any pending literals.
		c.fixLiterals(false)

		// Allocate data space
		dataLoc := c.allocateData(5)

		// Emit URX_LB_START
		c.appendOp(urxLbStart, dataLoc)

		// Emit URX_LB_CONT
		c.appendOp(urxLbCont, dataLoc)
		c.appendOp(urxReservedOp, 0) // MinMatchLength.  To be filled later.
		c.appendOp(urxReservedOp, 0) // MaxMatchLength.  To be filled later.

		// Emit the NOPs
		c.appendOp(urxNop, 0)
		c.appendOp(urxNop, 0)

		// On the Parentheses stack, start a new frame and add the postions
		//   of the URX_LB_CONT and the NOP.
		c.parenStack = append(c.parenStack, int(c.modeFlags))
		c.parenStack = append(c.parenStack, parenLookBehind)
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-2)
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-1)

	// The final two instructions will be added when the ')' is encountered.

	case doOpenLookBehindNeg:
		//   Compile a (?<! negated look-behind open paren.
		//
		//          Compiles to
		//              0       URX_LB_START     dataLoc    # Save entry stack, input len
		//              1       URX_LBN_CONT     dataLoc    # Iterate possible match positions
		//              2                        MinMatchLen
		//              3                        MaxMatchLen
		//              4                        continueLoc (9)
		//              5       URX_NOP          Standard '(' boilerplate.
		//              6       URX_NOP          Reserved slot for use with '|' ops within (block).
		//              7         <code for LookBehind expression>
		//              8       URX_LBN_END      dataLoc    # Check match len, cause a FAIL
		//              9       ...
		//
		//          Allocate a block of matcher data, to contain (when running a match)
		//              0:    Stack ptr on entry
		//              1:    Input Index on entry
		//              2:    fActiveStart, the active bounds start on entry.
		//              3:    fActiveLimit, the active bounds limit on entry.
		//              4:    Start index of match current match attempt.
		//          The first four items must match the layout of data for LA_START / LA_END

		// Generate match code for any pending literals.
		c.fixLiterals(false)

		// Allocate data space
		dataLoc := c.allocateData(5)

		// Emit URX_LB_START
		c.appendOp(urxLbStart, dataLoc)

		// Emit URX_LBN_CONT
		c.appendOp(urxLbnCount, dataLoc)
		c.appendOp(urxReservedOp, 0) // MinMatchLength.  To be filled later.
		c.appendOp(urxReservedOp, 0) // MaxMatchLength.  To be filled later.
		c.appendOp(urxReservedOp, 0) // Continue Loc.    To be filled later.

		// Emit the NOPs
		c.appendOp(urxNop, 0)
		c.appendOp(urxNop, 0)

		// On the Parentheses stack, start a new frame and add the postions
		//   of the URX_LB_CONT and the NOP.
		c.parenStack = append(c.parenStack, int(c.modeFlags))
		c.parenStack = append(c.parenStack, parenLookBehindN)
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-2)
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-1)

		// The final two instructions will be added when the ')' is encountered.

	case doConditionalExpr, doPerlInline:
		// Conditionals such as (?(1)a:b)
		// Perl inline-condtionals.  (?{perl code}a|b) We're not perl, no way to do them.
		c.error(Unimplemented)

	case doCloseParen:
		c.handleCloseParen()
		if len(c.parenStack) == 0 {
			//  Extra close paren, or missing open paren.
			c.error(MismatchedParen)
		}

	case doNOP:

	case doBadOpenParenType, doRuleError:
		c.error(RuleSyntax)

	case doMismatchedParenErr:
		c.error(MismatchedParen)

	case doPlus:
		//  Normal '+'  compiles to
		//     1.   stuff to be repeated  (already built)
		//     2.   jmp-sav 1
		//     3.   ...
		//
		//  Or, if the item to be repeated can match a zero length string,
		//     1.   STO_INP_LOC  data-loc
		//     2.      body of stuff to be repeated
		//     3.   JMP_SAV_X    2
		//     4.   ...

		//
		//  Or, if the item to be repeated is simple
		//     1.   Item to be repeated.
		//     2.   LOOP_SR_I    set number  (assuming repeated item is a set ref)
		//     3.   LOOP_C       stack location
		topLoc := c.blockTopLoc(false) // location of item #1

		// Check for simple constructs, which may get special optimized code.
		if topLoc == len(c.out.compiledPat)-1 {
			repeatedOp := c.out.compiledPat[topLoc]

			if repeatedOp.typ() == urxSetref {
				// Emit optimized code for [char set]+
				c.appendOp(urxLoopSrI, repeatedOp.value())
				frameLoc := c.allocateStackData(1)
				c.appendOp(urxLoopC, frameLoc)
				break
			}

			if repeatedOp.typ() == urxDotany || repeatedOp.typ() == urxDotanyAll || repeatedOp.typ() == urxDotanyUnix {
				// Emit Optimized code for .+ operations.
				loopOpI := c.buildOp(urxLoopDotI, 0)
				if repeatedOp.typ() == urxDotanyAll {
					// URX_LOOP_DOT_I operand is a flag indicating ". matches any" mode.
					loopOpI |= 1
				}
				if c.modeFlags&UnixLines != 0 {
					loopOpI |= 2
				}
				c.appendIns(loopOpI)
				frameLoc := c.allocateStackData(1)
				c.appendOp(urxLoopC, frameLoc)
				break
			}
		}

		// General case.

		// Check for minimum match length of zero, which requires
		//    extra loop-breaking code.
		if c.minMatchLength(topLoc, len(c.out.compiledPat)-1) == 0 {
			// Zero length match is possible.
			// Emit the code sequence that can handle it.
			c.insertOp(topLoc)
			frameLoc := c.allocateStackData(1)
			op := c.buildOp(urxStoInpLoc, frameLoc)
			c.out.compiledPat[topLoc] = op

			c.appendOp(urxJmpSavX, topLoc+1)
		} else {
			// Simpler code when the repeated body must match something non-empty
			c.appendOp(urxJmpSav, topLoc)
		}

	case doNGPlus:
		//  Non-greedy '+?'  compiles to
		//     1.   stuff to be repeated  (already built)
		//     2.   state-save  1
		//     3.   ...
		topLoc := c.blockTopLoc(false)
		c.appendOp(urxStateSave, topLoc)

	case doOpt:
		// Normal (greedy) ? quantifier.
		//  Compiles to
		//     1. state save 3
		//     2.    body of optional block
		//     3. ...
		// Insert the state save into the compiled pattern, and we're done.
		saveStateLoc := c.blockTopLoc(true)
		saveStateOp := c.buildOp(urxStateSave, len(c.out.compiledPat))
		c.out.compiledPat[saveStateLoc] = saveStateOp

	case doNGOpt:
		// Non-greedy ?? quantifier
		//   compiles to
		//    1.  jmp   4
		//    2.     body of optional block
		//    3   jmp   5
		//    4.  state save 2
		//    5    ...
		//  This code is less than ideal, with two jmps instead of one, because we can only
		//  insert one instruction at the top of the block being iterated.
		jmp1Loc := c.blockTopLoc(true)
		jmp2Loc := len(c.out.compiledPat)

		jmp1Op := c.buildOp(urxJmp, jmp2Loc+1)
		c.out.compiledPat[jmp1Loc] = jmp1Op

		c.appendOp(urxJmp, jmp2Loc+2)
		c.appendOp(urxStateSave, jmp1Loc+1)

	case doStar:
		// Normal (greedy) * quantifier.
		// Compiles to
		//       1.   STATE_SAVE   4
		//       2.      body of stuff being iterated over
		//       3.   JMP_SAV      2
		//       4.   ...
		//
		// Or, if the body is a simple [Set],
		//       1.   LOOP_SR_I    set number
		//       2.   LOOP_C       stack location
		//       ...
		//
		// Or if this is a .*
		//       1.   LOOP_DOT_I    (. matches all mode flag)
		//       2.   LOOP_C        stack location
		//
		// Or, if the body can match a zero-length string, to inhibit infinite loops,
		//       1.   STATE_SAVE   5
		//       2.   STO_INP_LOC  data-loc
		//       3.      body of stuff
		//       4.   JMP_SAV_X    2
		//       5.   ...
		// location of item #1, the STATE_SAVE
		topLoc := c.blockTopLoc(false)

		// Check for simple *, where the construct being repeated
		//   compiled to single opcode, and might be optimizable.
		if topLoc == len(c.out.compiledPat)-1 {
			repeatedOp := c.out.compiledPat[topLoc]

			if repeatedOp.typ() == urxSetref {
				// Emit optimized code for a [char set]*
				loopOpI := c.buildOp(urxLoopSrI, repeatedOp.value())
				c.out.compiledPat[topLoc] = loopOpI
				dataLoc := c.allocateStackData(1)
				c.appendOp(urxLoopC, dataLoc)
				break
			}

			if repeatedOp.typ() == urxDotany || repeatedOp.typ() == urxDotanyAll || repeatedOp.typ() == urxDotanyUnix {
				// Emit Optimized code for .* operations.
				loopOpI := c.buildOp(urxLoopDotI, 0)
				if repeatedOp.typ() == urxDotanyAll {
					// URX_LOOP_DOT_I operand is a flag indicating . matches any mode.
					loopOpI |= 1
				}
				if (c.modeFlags & UnixLines) != 0 {
					loopOpI |= 2
				}
				c.out.compiledPat[topLoc] = loopOpI
				dataLoc := c.allocateStackData(1)
				c.appendOp(urxLoopC, dataLoc)
				break
			}
		}

		// Emit general case code for this *
		// The optimizations did not apply.

		saveStateLoc := c.blockTopLoc(true)
		jmpOp := c.buildOp(urxJmpSav, saveStateLoc+1)

		// Check for minimum match length of zero, which requires
		//    extra loop-breaking code.
		if c.minMatchLength(saveStateLoc, len(c.out.compiledPat)-1) == 0 {
			c.insertOp(saveStateLoc)
			dataLoc := c.allocateStackData(1)

			op := c.buildOp(urxStoInpLoc, dataLoc)
			c.out.compiledPat[saveStateLoc+1] = op
			jmpOp = c.buildOp(urxJmpSavX, saveStateLoc+2)
		}

		// Locate the position in the compiled pattern where the match will continue
		//   after completing the *.   (4 or 5 in the comment above)
		continueLoc := len(c.out.compiledPat) + 1

		// Put together the save state op and store it into the compiled code.
		saveStateOp := c.buildOp(urxStateSave, continueLoc)
		c.out.compiledPat[saveStateLoc] = saveStateOp

		// Append the URX_JMP_SAV or URX_JMPX operation to the compiled pattern.
		c.appendIns(jmpOp)

	case doNGStar:
		// Non-greedy *? quantifier
		// compiles to
		//     1.   JMP    3
		//     2.      body of stuff being iterated over
		//     3.   STATE_SAVE  2
		//     4    ...
		jmpLoc := c.blockTopLoc(true)     // loc  1.
		saveLoc := len(c.out.compiledPat) // loc  3.
		jmpOp := c.buildOp(urxJmp, saveLoc)
		c.out.compiledPat[jmpLoc] = jmpOp
		c.appendOp(urxStateSave, jmpLoc+1)

	case doIntervalInit:
		// The '{' opening an interval quantifier was just scanned.
		// Init the counter varaiables that will accumulate the values as the digits
		//    are scanned.
		c.intervalLow = 0
		c.intervalUpper = -1

	case doIntevalLowerDigit:
		// Scanned a digit from the lower value of an {lower,upper} interval
		digitValue := uCharDigitValue(c.c.char)
		val := int64(c.intervalLow)*10 + digitValue
		if val > math.MaxInt32 {
			c.error(NumberTooBig)
		} else {
			c.intervalLow = int(val)
		}

	case doIntervalUpperDigit:
		// Scanned a digit from the upper value of an {lower,upper} interval
		if c.intervalUpper < 0 {
			c.intervalUpper = 0
		}
		digitValue := uCharDigitValue(c.c.char)
		val := int64(c.intervalUpper)*10 + digitValue
		if val > math.MaxInt32 {
			c.error(NumberTooBig)
		} else {
			c.intervalUpper = int(val)
		}

	case doIntervalSame:
		// Scanned a single value interval like {27}.  Upper = Lower.
		c.intervalUpper = c.intervalLow

	case doInterval:
		// Finished scanning a normal {lower,upper} interval.  Generate the code for it.
		if !c.compileInlineInterval() {
			c.compileInterval(urxCtrInit, utxCtrLoop)
		}

	case doPossessiveInterval:
		// Finished scanning a Possessive {lower,upper}+ interval.  Generate the code for it.

		// Remember the loc for the top of the block being looped over.
		//   (Can not reserve a slot in the compiled pattern at this time, because
		//    compileInterval needs to reserve also, and blockTopLoc can only reserve
		//    once per block.)
		topLoc := c.blockTopLoc(false)

		// Produce normal looping code.
		c.compileInterval(urxCtrInit, utxCtrLoop)

		// Surround the just-emitted normal looping code with a STO_SP ... LD_SP
		//  just as if the loop was inclosed in atomic parentheses.

		// First the STO_SP before the start of the loop
		c.insertOp(topLoc)

		varLoc := c.allocateData(1) // Reserve a data location for saving the
		op := c.buildOp(urxStoSp, varLoc)
		c.out.compiledPat[topLoc] = op

		var loopOp instruction
		loopOp, c.out.compiledPat = stackPop(c.out.compiledPat)
		if loopOp.typ() != utxCtrLoop || loopOp.value() != topLoc {
			panic("bad instruction at the end of compiled pattern")
		}

		loopOp++ // point LoopOp after the just-inserted STO_SP
		c.appendIns(loopOp)

		// Then the LD_SP after the end of the loop
		c.appendOp(urxLdSp, varLoc)

	case doNGInterval:
		// Finished scanning a non-greedy {lower,upper}? interval.  Generate the code for it.
		c.compileInterval(urxCtrInitNg, urxCtrLoopNg)

	case doIntervalError:
		c.error(BadInterval)

	case doLiteralChar:
		// We've just scanned a "normal" character from the pattern,
		c.literalChar(c.c.char)

	case doEscapedLiteralChar:
		// We've just scanned an backslashed escaped character with  no
		//   special meaning.  It represents itself.
		if (c.modeFlags&ErrorOnUnknownEscapes) != 0 && ((c.c.char >= 0x41 && c.c.char <= 0x5A) || /* in [A-Z] */ (c.c.char >= 0x61 && c.c.char <= 0x7a)) { // in [a-z]
			c.error(BadEscapeSequence)
		}
		c.literalChar(c.c.char)

	case doDotAny:
		// scanned a ".",  match any single character.
		c.fixLiterals(false)
		if (c.modeFlags & DotAll) != 0 {
			c.appendOp(urxDotanyAll, 0)
		} else if (c.modeFlags & UnixLines) != 0 {
			c.appendOp(urxDotanyUnix, 0)
		} else {
			c.appendOp(urxDotany, 0)
		}

	case doCaret:
		c.fixLiterals(false)
		if (c.modeFlags&Multiline) == 0 && (c.modeFlags&UnixLines) == 0 {
			c.appendOp(urxCaret, 0)
		} else if (c.modeFlags&Multiline) != 0 && (c.modeFlags&UnixLines) == 0 {
			c.appendOp(urxCaretM, 0)
		} else if (c.modeFlags&Multiline) == 0 && (c.modeFlags&UnixLines) != 0 {
			c.appendOp(urxCaret, 0) // Only testing true start of input.
		} else if (c.modeFlags&Multiline) != 0 && (c.modeFlags&UnixLines) != 0 {
			c.appendOp(urxCaretMUnix, 0)
		}

	case doDollar:
		c.fixLiterals(false)
		if (c.modeFlags&Multiline) == 0 && (c.modeFlags&UnixLines) == 0 {
			c.appendOp(urxDollar, 0)
		} else if (c.modeFlags&Multiline) != 0 && (c.modeFlags&UnixLines) == 0 {
			c.appendOp(urxDollarM, 0)
		} else if (c.modeFlags&Multiline) == 0 && (c.modeFlags&UnixLines) != 0 {
			c.appendOp(urxDollarD, 0)
		} else if (c.modeFlags&Multiline) != 0 && (c.modeFlags&UnixLines) != 0 {
			c.appendOp(urxDollarMd, 0)
		}

	case doBackslashA:
		c.fixLiterals(false)
		c.appendOp(urxCaret, 0)

	case doBackslashB:
		if !BreakIteration {
			if (c.modeFlags & UWord) != 0 {
				c.error(Unimplemented)
			}
		}
		c.fixLiterals(false)
		if c.modeFlags&UWord != 0 {
			c.appendOp(urxBackslashBu, 1)
		} else {
			c.appendOp(urxBackslashB, 1)
		}

	case doBackslashb:
		if !BreakIteration {
			if (c.modeFlags & UWord) != 0 {
				c.error(Unimplemented)
			}
		}
		c.fixLiterals(false)
		if c.modeFlags&UWord != 0 {
			c.appendOp(urxBackslashBu, 0)
		} else {
			c.appendOp(urxBackslashB, 0)
		}

	case doBackslashD:
		c.fixLiterals(false)
		c.appendOp(urxBackslashD, 1)

	case doBackslashd:
		c.fixLiterals(false)
		c.appendOp(urxBackslashD, 0)

	case doBackslashG:
		c.fixLiterals(false)
		c.appendOp(urxBackslashG, 0)

	case doBackslashH:
		c.fixLiterals(false)
		c.appendOp(urxBackslashH, 1)

	case doBackslashh:
		c.fixLiterals(false)
		c.appendOp(urxBackslashH, 0)

	case doBackslashR:
		c.fixLiterals(false)
		c.appendOp(urxBackslashR, 0)

	case doBackslashS:
		c.fixLiterals(false)
		c.appendOp(urxStatSetrefN, urxIsspaceSet)

	case doBackslashs:
		c.fixLiterals(false)
		c.appendOp(urxStaticSetref, urxIsspaceSet)

	case doBackslashV:
		c.fixLiterals(false)
		c.appendOp(urxBackslashV, 1)

	case doBackslashv:
		c.fixLiterals(false)
		c.appendOp(urxBackslashV, 0)

	case doBackslashW:
		c.fixLiterals(false)
		c.appendOp(urxStatSetrefN, urxIswordSet)

	case doBackslashw:
		c.fixLiterals(false)
		c.appendOp(urxStaticSetref, urxIswordSet)

	case doBackslashX:
		if !BreakIteration {
			// Grapheme Cluster Boundary requires ICU break iteration.
			c.error(Unimplemented)
		}
		c.fixLiterals(false)
		c.appendOp(urxBackslashX, 0)

	case doBackslashZ:
		c.fixLiterals(false)
		c.appendOp(urxDollar, 0)

	case doBackslashz:
		c.fixLiterals(false)
		c.appendOp(urxBackslashZ, 0)

	case doEscapeError:
		c.error(BadEscapeSequence)

	case doExit:
		c.fixLiterals(false)
		return false

	case doProperty:
		c.fixLiterals(false)
		theSet := c.scanProp()
		c.compileSet(theSet)

	case doNamedChar:
		ch := c.scanNamedChar()
		c.literalChar(ch)

	case doBackRef:
		// BackReference.  Somewhat unusual in that the front-end can not completely parse
		//                 the regular expression, because the number of digits to be consumed
		//                 depends on the number of capture groups that have been defined.  So
		//                 we have to do it here instead.
		numCaptureGroups := len(c.out.groupMap)
		groupNum := int64(0)
		ch := c.c.char

		for {
			// Loop once per digit, for max allowed number of digits in a back reference.
			digit := uCharDigitValue(ch)
			groupNum = groupNum*10 + digit
			if groupNum >= int64(numCaptureGroups) {
				break
			}
			ch = c.peekCharLL()
			if !staticRuleSet[ruleSetDigitChar-128].ContainsRune(ch) {
				break
			}
			c.nextCharLL()
		}

		// Scan of the back reference in the source regexp is complete.  Now generate
		//  the compiled code for it.
		// Because capture groups can be forward-referenced by back-references,
		//  we fill the operand with the capture group number.  At the end
		//  of compilation, it will be changed to the variable's location.
		if groupNum == 0 {
			panic("\\0 begins an octal escape sequence, and shouldn't enter this code path at all")
		}
		c.fixLiterals(false)
		if (c.modeFlags & CaseInsensitive) != 0 {
			c.appendOp(urxBackrefI, int(groupNum))
		} else {
			c.appendOp(urxBackref, int(groupNum))
		}

	case doBeginNamedBackRef:
		if c.captureName != nil {
			panic("should not replace capture name")
		}
		c.captureName = &strings.Builder{}

	case doContinueNamedBackRef:
		c.captureName.WriteRune(c.c.char)

	case doCompleteNamedBackRef:
		{
			groupNumber := c.out.namedCaptureMap[c.captureName.String()]
			if groupNumber == 0 {
				// Group name has not been defined.
				//   Could be a forward reference. If we choose to support them at some
				//   future time, extra mechanism will be required at this point.
				c.error(InvalidCaptureGroupName)
			} else {
				// Given the number, handle identically to a \n numbered back reference.
				// See comments above, under doBackRef
				c.fixLiterals(false)
				if (c.modeFlags & CaseInsensitive) != 0 {
					c.appendOp(urxBackrefI, groupNumber)
				} else {
					c.appendOp(urxBackref, groupNumber)
				}
			}
			c.captureName = nil
		}

	case doPossessivePlus:
		// Possessive ++ quantifier.
		// Compiles to
		//       1.   STO_SP
		//       2.      body of stuff being iterated over
		//       3.   STATE_SAVE 5
		//       4.   JMP        2
		//       5.   LD_SP
		//       6.   ...
		//
		//  Note:  TODO:  This is pretty inefficient.  A mass of saved state is built up
		//                then unconditionally discarded.  Perhaps introduce a new opcode.  Ticket 6056
		//
		// Emit the STO_SP
		topLoc := c.blockTopLoc(true)
		stoLoc := c.allocateData(1) // Reserve the data location for storing save stack ptr.
		op := c.buildOp(urxStoSp, stoLoc)
		c.out.compiledPat[topLoc] = op

		// Emit the STATE_SAVE
		c.appendOp(urxStateSave, len(c.out.compiledPat)+2)

		// Emit the JMP
		c.appendOp(urxJmp, topLoc+1)

		// Emit the LD_SP
		c.appendOp(urxLdSp, stoLoc)

	case doPossessiveStar:
		// Possessive *+ quantifier.
		// Compiles to
		//       1.   STO_SP       loc
		//       2.   STATE_SAVE   5
		//       3.      body of stuff being iterated over
		//       4.   JMP          2
		//       5.   LD_SP        loc
		//       6    ...
		// TODO:  do something to cut back the state stack each time through the loop.
		// Reserve two slots at the top of the block.
		topLoc := c.blockTopLoc(true)
		c.insertOp(topLoc)

		// emit   STO_SP     loc
		stoLoc := c.allocateData(1) // Reserve the data location for storing save stack ptr.
		op := c.buildOp(urxStoSp, stoLoc)
		c.out.compiledPat[topLoc] = op

		// Emit the SAVE_STATE   5
		L7 := len(c.out.compiledPat) + 1
		op = c.buildOp(urxStateSave, L7)
		c.out.compiledPat[topLoc+1] = op

		// Append the JMP operation.
		c.appendOp(urxJmp, topLoc+1)

		// Emit the LD_SP       loc
		c.appendOp(urxLdSp, stoLoc)

	case doPossessiveOpt:
		// Possessive  ?+ quantifier.
		//  Compiles to
		//     1. STO_SP      loc
		//     2. SAVE_STATE  5
		//     3.    body of optional block
		//     4. LD_SP       loc
		//     5. ...
		//
		// Reserve two slots at the top of the block.
		topLoc := c.blockTopLoc(true)
		c.insertOp(topLoc)

		// Emit the STO_SP
		stoLoc := c.allocateData(1) // Reserve the data location for storing save stack ptr.
		op := c.buildOp(urxStoSp, stoLoc)
		c.out.compiledPat[topLoc] = op

		// Emit the SAVE_STATE
		continueLoc := len(c.out.compiledPat) + 1
		op = c.buildOp(urxStateSave, continueLoc)
		c.out.compiledPat[topLoc+1] = op

		// Emit the LD_SP
		c.appendOp(urxLdSp, stoLoc)

	case doBeginMatchMode:
		c.newModeFlags = c.modeFlags
		c.setModeFlag = true
	case doMatchMode: //  (?i)    and similar
		var bit RegexpFlag
		switch c.c.char {
		case 0x69: /* 'i' */
			bit = CaseInsensitive
		case 0x64: /* 'd' */
			bit = UnixLines
		case 0x6d: /* 'm' */
			bit = Multiline
		case 0x73: /* 's' */
			bit = DotAll
		case 0x75: /* 'u' */
			bit = 0 /* Unicode casing */
		case 0x77: /* 'w' */
			bit = UWord
		case 0x78: /* 'x' */
			bit = Comments
		case 0x2d: /* '-' */
			c.setModeFlag = false
		default:
			// Should never happen.  Other chars are filtered out by the scanner.
			panic("unreachable")
		}
		if c.setModeFlag {
			c.newModeFlags |= bit
		} else {
			c.newModeFlags &= ^bit
		}

	case doSetMatchMode:
		// Emit code to match any pending literals, using the not-yet changed match mode.
		c.fixLiterals(false)

		// We've got a (?i) or similar.  The match mode is being changed, but
		//   the change is not scoped to a parenthesized block.
		if c.newModeFlags >= 0 {
			panic("cNewModeFlags not properly initialized")
		}
		c.modeFlags = c.newModeFlags

	case doMatchModeParen:
		// We've got a (?i: or similar.  Begin a parenthesized block, save old
		//   mode flags so they can be restored at the close of the block.
		//
		//   Compile to a
		//      - NOP, which later may be replaced by a save-state if the
		//         parenthesized group gets a * quantifier, followed by
		//      - NOP, which may later be replaced by a save-state if there
		//             is an '|' alternation within the parens.
		c.fixLiterals(false)
		c.appendOp(urxNop, 0)
		c.appendOp(urxNop, 0)

		// On the Parentheses stack, start a new frame and add the postions
		//   of the two NOPs (a normal non-capturing () frame, except for the
		//   saving of the orignal mode flags.)
		c.parenStack = append(c.parenStack, int(c.modeFlags))
		c.parenStack = append(c.parenStack, parenFlags)
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-2)
		c.parenStack = append(c.parenStack, len(c.out.compiledPat)-1)

		// Set the current mode flags to the new values.
		if c.newModeFlags >= 0 {
			panic("cNewModeFlags not properly initialized")
		}
		c.modeFlags = c.newModeFlags

	case doBadModeFlag:
		c.error(InvalidFlag)

	case doSuppressComments:
		// We have just scanned a '(?'.  We now need to prevent the character scanner from
		// treating a '#' as a to-the-end-of-line comment.
		//   (This Perl compatibility just gets uglier and uglier to do...)
		c.eolComments = false

	case doSetAddAmp:
		set := c.setStack[len(c.setStack)-1]
		set.AddRune(chAmp)

	case doSetAddDash:
		set := c.setStack[len(c.setStack)-1]
		set.AddRune(chDash)

	case doSetBackslashs:
		set := c.setStack[len(c.setStack)-1]
		set.AddAll(staticPropertySets[urxIsspaceSet])

	case doSetBackslashS:
		sset := uset.New()
		sset.AddAll(staticPropertySets[urxIsspaceSet]) // TODO: add latin1 spaces
		sset.Complement()

		set := c.setStack[len(c.setStack)-1]
		set.AddAll(sset)

	case doSetBackslashd:
		set := c.setStack[len(c.setStack)-1]
		c.err = uprops.AddCategory(set, uchar.GcNdMask)

	case doSetBackslashD:
		digits := uset.New()
		c.err = uprops.ApplyIntPropertyValue(digits, uprops.UCharGeneralCategoryMask, int32(uchar.GcNdMask))
		digits.Complement()
		set := c.setStack[len(c.setStack)-1]
		set.AddAll(digits)

	case doSetBackslashh:
		h := uset.New()
		c.err = uprops.ApplyIntPropertyValue(h, uprops.UCharGeneralCategoryMask, int32(uchar.GcZsMask))
		h.AddRune(9) // Tab

		set := c.setStack[len(c.setStack)-1]
		set.AddAll(h)

	case doSetBackslashH:
		h := uset.New()
		c.err = uprops.ApplyIntPropertyValue(h, uprops.UCharGeneralCategoryMask, int32(uchar.GcZsMask))
		h.AddRune(9) // Tab
		h.Complement()

		set := c.setStack[len(c.setStack)-1]
		set.AddAll(h)

	case doSetBackslashv:
		set := c.setStack[len(c.setStack)-1]
		set.AddRuneRange(0x0a, 0x0d) // add range
		set.AddRune(0x85)
		set.AddRuneRange(0x2028, 0x2029)

	case doSetBackslashV:
		v := uset.New()
		v.AddRuneRange(0x0a, 0x0d) // add range
		v.AddRune(0x85)
		v.AddRuneRange(0x2028, 0x2029)
		v.Complement()

		set := c.setStack[len(c.setStack)-1]
		set.AddAll(v)

	case doSetBackslashw:
		set := c.setStack[len(c.setStack)-1]
		set.AddAll(staticPropertySets[urxIswordSet])

	case doSetBackslashW:
		sset := uset.New()
		sset.AddAll(staticPropertySets[urxIswordSet])
		sset.Complement()

		set := c.setStack[len(c.setStack)-1]
		set.AddAll(sset)

	case doSetBegin:
		c.fixLiterals(false)
		c.setStack = append(c.setStack, uset.New())
		c.setOpStack = append(c.setOpStack, setStart)
		if (c.modeFlags & CaseInsensitive) != 0 {
			c.setOpStack = append(c.setOpStack, setCaseClose)
		}

	case doSetBeginDifference1:
		//  We have scanned something like [[abc]-[
		//  Set up a new UnicodeSet for the set beginning with the just-scanned '['
		//  Push a Difference operator, which will cause the new set to be subtracted from what
		//    went before once it is created.
		c.setPushOp(setDifference1)
		c.setOpStack = append(c.setOpStack, setStart)
		if (c.modeFlags & CaseInsensitive) != 0 {
			c.setOpStack = append(c.setOpStack, setCaseClose)
		}

	case doSetBeginIntersection1:
		//  We have scanned something like  [[abc]&[
		//   Need both the '&' operator and the open '[' operator.
		c.setPushOp(setIntersection1)
		c.setOpStack = append(c.setOpStack, setStart)
		if (c.modeFlags & CaseInsensitive) != 0 {
			c.setOpStack = append(c.setOpStack, setCaseClose)
		}

	case doSetBeginUnion:
		//  We have scanned something like  [[abc][
		//     Need to handle the union operation explicitly [[abc] | [
		c.setPushOp(setUnion)
		c.setOpStack = append(c.setOpStack, setStart)
		if (c.modeFlags & CaseInsensitive) != 0 {
			c.setOpStack = append(c.setOpStack, setCaseClose)
		}

	case doSetDifference2:
		// We have scanned something like [abc--
		//   Consider this to unambiguously be a set difference operator.
		c.setPushOp(setDifference2)

	case doSetEnd:
		// Have encountered the ']' that closes a set.
		//    Force the evaluation of any pending operations within this set,
		//    leave the completed set on the top of the set stack.
		c.setEval(setEnd)
		var start setOperation
		start, c.setOpStack = stackPop(c.setOpStack)
		if start != setStart {
			panic("bad set operation in stack")
		}

	case doSetFinish:
		// Finished a complete set expression, including all nested sets.
		//   The close bracket has already triggered clearing out pending set operators,
		//    the operator stack should be empty and the operand stack should have just
		//    one entry, the result set.
		if len(c.setOpStack) > 0 {
			panic("expected setOpStack to be empty")
		}
		var set *uset.UnicodeSet
		set, c.setStack = stackPop(c.setStack)
		c.compileSet(set)

	case doSetIntersection2:
		// Have scanned something like [abc&&
		c.setPushOp(setIntersection2)

	case doSetLiteral:
		// Union the just-scanned literal character into the set being built.
		//    This operation is the highest precedence set operation, so we can always do
		//    it immediately, without waiting to see what follows.  It is necessary to perform
		//    any pending '-' or '&' operation first, because these have the same precedence
		//    as union-ing in a literal'
		c.setEval(setUnion)
		set := c.setStack[len(c.setStack)-1]
		set.AddRune(c.c.char)
		c.lastSetLiteral = c.c.char

	case doSetLiteralEscaped:
		// A back-slash escaped literal character was encountered.
		// Processing is the same as with setLiteral, above, with the addition of
		//  the optional check for errors on escaped ASCII letters.
		if (c.modeFlags&ErrorOnUnknownEscapes) != 0 &&
			((c.c.char >= 0x41 && c.c.char <= 0x5A) || // in [A-Z]
				(c.c.char >= 0x61 && c.c.char <= 0x7a)) { // in [a-z]
			c.error(BadEscapeSequence)
		}
		c.setEval(setUnion)
		set := c.setStack[len(c.setStack)-1]
		set.AddRune(c.c.char)
		c.lastSetLiteral = c.c.char

	case doSetNamedChar:
		// Scanning a \N{UNICODE CHARACTER NAME}
		//  Aside from the source of the character, the processing is identical to doSetLiteral,
		//    above.
		ch := c.scanNamedChar()
		c.setEval(setUnion)
		set := c.setStack[len(c.setStack)-1]
		set.AddRune(ch)
		c.lastSetLiteral = ch

	case doSetNamedRange:
		// We have scanned literal-\N{CHAR NAME}.  Add the range to the set.
		// The left character is already in the set, and is saved in fLastSetLiteral.
		// The right side needs to be picked up, the scan is at the 'N'.
		// Lower Limit > Upper limit being an error matches both Java
		//        and ICU UnicodeSet behavior.
		ch := c.scanNamedChar()
		if c.err == nil && (c.lastSetLiteral == -1 || c.lastSetLiteral > ch) {
			c.error(InvalidRange)
		}
		set := c.setStack[len(c.setStack)-1]
		set.AddRuneRange(c.lastSetLiteral, ch)
		c.lastSetLiteral = ch

	case doSetNegate:
		// Scanned a '^' at the start of a set.
		// Push the negation operator onto the set op stack.
		// A twist for case-insensitive matching:
		//   the case closure operation must happen _before_ negation.
		//   But the case closure operation will already be on the stack if it's required.
		//   This requires checking for case closure, and swapping the stack order
		//    if it is present.
		tosOp := c.setOpStack[len(c.setOpStack)-1]
		if tosOp == setCaseClose {
			_, c.setOpStack = stackPop(c.setOpStack)
			c.setOpStack = append(c.setOpStack, setNegation)
			c.setOpStack = append(c.setOpStack, setCaseClose)
		} else {
			c.setOpStack = append(c.setOpStack, setNegation)
		}

	case doSetNoCloseError:
		c.error(MissingCloseBracket)

	case doSetOpError:
		c.error(RuleSyntax) //  -- or && at the end of a set.  Illegal.

	case doSetPosixProp:
		if set := c.scanPosixProp(); set != nil {
			c.setStack[len(c.setStack)-1].AddAll(set)
		}

	case doSetProp:
		//  Scanned a \p \P within [brackets].
		if set := c.scanProp(); set != nil {
			c.setStack[len(c.setStack)-1].AddAll(set)
		}

	case doSetRange:
		// We have scanned literal-literal.  Add the range to the set.
		// The left character is already in the set, and is saved in fLastSetLiteral.
		// The right side is the current character.
		// Lower Limit > Upper limit being an error matches both Java
		//        and ICU UnicodeSet behavior.

		if c.lastSetLiteral == -1 || c.lastSetLiteral > c.c.char {
			c.error(InvalidRange)
		}
		c.setStack[len(c.setStack)-1].AddRuneRange(c.lastSetLiteral, c.c.char)

	default:
		panic("unexpected OP in parser")
	}

	return c.err == nil
}

func uCharDigitValue(char rune) int64 {
	if char >= '0' && char <= '9' {
		return int64(char - '0')
	}
	return -1
}

func stackPop[T any](stack []T) (T, []T) {
	var out T
	if len(stack) > 0 {
		out = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
	}
	return out, stack
}

func (c *compiler) error(e CompileErrorCode) {
	c.err = &CompileError{
		Code:    e,
		Line:    c.lineNum,
		Offset:  c.charNum,
		Context: c.out.pattern,
	}
}

func (c *compiler) stripNOPs() {
	if c.err != nil {
		return
	}

	end := len(c.out.compiledPat)
	deltas := make([]int, 0, end)

	// Make a first pass over the code, computing the amount that things
	//   will be offset at each location in the original code.
	var loc, d int
	for loc = 0; loc < end; loc++ {
		deltas = append(deltas, d)
		op := c.out.compiledPat[loc]
		if op.typ() == urxNop {
			d++
		}
	}

	// Make a second pass over the code, removing the NOPs by moving following
	//  code up, and patching operands that refer to code locations that
	//  are being moved.  The array of offsets from the first step is used
	//  to compute the new operand values.
	var src, dst int
	for src = 0; src < end; src++ {
		op := c.out.compiledPat[src]
		opType := op.typ()

		switch opType {
		case urxNop:
			// skip

		case urxStateSave,
			urxJmp,
			utxCtrLoop,
			urxCtrLoopNg,
			urxRelocOprnd,
			urxJmpx,
			urxJmpSav,
			urxJmpSavX:
			// These are instructions with operands that refer to code locations.
			operandAddress := op.value()
			fixedOperandAddress := operandAddress - deltas[operandAddress]
			op = c.buildOp(opType, fixedOperandAddress)
			c.out.compiledPat[dst] = op
			dst++

		case urxBackref, urxBackrefI:
			where := op.value()
			if where > len(c.out.groupMap) {
				c.error(InvalidBackRef)
				break
			}

			where = int(c.out.groupMap[where-1])
			op = c.buildOp(opType, where)
			c.out.compiledPat[dst] = op
			dst++
			c.out.needsAltInput = true

		case urxReservedOp,
			urxReservedOpN,
			urxBacktrack,
			urxEnd,
			urxOnechar,
			urxString,
			urxStringLen,
			urxStartCapture,
			urxEndCapture,
			urxStaticSetref,
			urxStatSetrefN,
			urxSetref,
			urxDotany,
			urxFail,
			urxBackslashB,
			urxBackslashBu,
			urxBackslashG,
			urxBackslashX,
			urxBackslashZ,
			urxDotanyAll,
			urxBackslashD,
			urxCaret,
			urxDollar,
			urxCtrInit,
			urxCtrInitNg,
			urxDotanyUnix,
			urxStoSp,
			urxLdSp,
			urxStoInpLoc,
			urxLaStart,
			urxLaEnd,
			urcOnecharI,
			urxStringI,
			urxDollarM,
			urxCaretM,
			urxCaretMUnix,
			urxLbStart,
			urxLbCont,
			urxLbEnd,
			urxLbnCount,
			urxLbnEnd,
			urxLoopSrI,
			urxLoopDotI,
			urxLoopC,
			urxDollarD,
			urxDollarMd,
			urxBackslashH,
			urxBackslashR,
			urxBackslashV:
			// These instructions are unaltered by the relocation.
			c.out.compiledPat[dst] = op
			dst++

		default:
			// Some op is unaccounted for.
			panic("unreachable")
		}
	}

	c.out.compiledPat = c.out.compiledPat[:dst]
}

func (c *compiler) matchStartType() {
	var loc int               // Location in the pattern of the current op being processed.
	var currentLen int32      // Minimum length of a match to this point (loc) in the pattern
	var numInitialStrings int // Number of strings encountered that could match at start.
	var atStart = true        // True if no part of the pattern yet encountered
	//   could have advanced the position in a match.
	//   (Maximum match length so far == 0)

	// forwardedLength is a vector holding minimum-match-length values that
	//   are propagated forward in the pattern by JMP or STATE_SAVE operations.
	//   It must be one longer than the pattern being checked because some  ops
	//   will jmp to a end-of-block+1 location from within a block, and we must
	//   count those when checking the block.
	end := len(c.out.compiledPat)
	forwardedLength := make([]int32, end+1)

	for loc = 3; loc < end; loc++ {
		forwardedLength[loc] = math.MaxInt32
	}

	for loc = 3; loc < end; loc++ {
		op := c.out.compiledPat[loc]
		opType := op.typ()

		// The loop is advancing linearly through the pattern.
		// If the op we are now at was the destination of a branch in the pattern,
		// and that path has a shorter minimum length than the current accumulated value,
		// replace the current accumulated value.
		if forwardedLength[loc] < currentLen {
			currentLen = forwardedLength[loc]
		}

		switch opType {
		// Ops that don't change the total length matched
		case urxReservedOp,
			urxEnd,
			urxFail,
			urxStringLen,
			urxNop,
			urxStartCapture,
			urxEndCapture,
			urxBackslashB,
			urxBackslashBu,
			urxBackslashG,
			urxBackslashZ,
			urxDollar,
			urxDollarM,
			urxDollarD,
			urxDollarMd,
			urxRelocOprnd,
			urxStoInpLoc,
			urxBackref, // BackRef.  Must assume that it might be a zero length match
			urxBackrefI,
			urxStoSp, // Setup for atomic or possessive blocks.  Doesn't change what can match.
			urxLdSp:
			// skip

		case urxCaret:
			if atStart {
				c.out.startType = startStart
			}

		case urxCaretM, urxCaretMUnix:
			if atStart {
				c.out.startType = startLine
			}

		case urxOnechar:
			if currentLen == 0 {
				// This character could appear at the start of a match.
				//   Add it to the set of possible starting characters.
				c.out.initialChars.AddRune(op.value32())
				numInitialStrings += 2
			}
			currentLen = safeIncrement(currentLen, 1)
			atStart = false

		case urxSetref:
			if currentLen == 0 {
				sn := op.value()
				set := c.out.sets[sn]
				c.out.initialChars.AddAll(set)
				numInitialStrings += 2
			}
			currentLen = safeIncrement(currentLen, 1)
			atStart = false

		case urxLoopSrI:
			// [Set]*, like a SETREF, above, in what it can match,
			//  but may not match at all, so currentLen is not incremented.
			if currentLen == 0 {
				sn := op.value()
				set := c.out.sets[sn]
				c.out.initialChars.AddAll(set)
				numInitialStrings += 2
			}
			atStart = false

		case urxLoopDotI:
			if currentLen == 0 {
				// .* at the start of a pattern.
				//    Any character can begin the match.
				c.out.initialChars.Clear()
				c.out.initialChars.Complement()
				numInitialStrings += 2
			}
			atStart = false

		case urxStaticSetref:
			if currentLen == 0 {
				sn := op.value()
				c.out.initialChars.AddAll(staticPropertySets[sn])
				numInitialStrings += 2
			}
			currentLen = safeIncrement(currentLen, 1)
			atStart = false

		case urxStatSetrefN:
			if currentLen == 0 {
				sn := op.value()
				sc := uset.New()
				sc.AddAll(staticPropertySets[sn])
				sc.Complement()

				c.out.initialChars.AddAll(sc)
				numInitialStrings += 2
			}
			currentLen = safeIncrement(currentLen, 1)
			atStart = false

		case urxBackslashD:
			// Digit Char
			if currentLen == 0 {
				s := uset.New()
				c.err = uprops.ApplyIntPropertyValue(s, uprops.UCharGeneralCategoryMask, int32(uchar.GcNdMask))
				if op.value() != 0 {
					s.Complement()
				}
				c.out.initialChars.AddAll(s)
				numInitialStrings += 2
			}
			currentLen = safeIncrement(currentLen, 1)
			atStart = false

		case urxBackslashH:
			// Horiz white space
			if currentLen == 0 {
				s := uset.New()
				c.err = uprops.ApplyIntPropertyValue(s, uprops.UCharGeneralCategoryMask, int32(uchar.GcZsMask))
				s.AddRune(9) // Tab
				if op.value() != 0 {
					s.Complement()
				}
				c.out.initialChars.AddAll(s)
				numInitialStrings += 2
			}
			currentLen = safeIncrement(currentLen, 1)
			atStart = false

		case urxBackslashR, // Any line ending sequence
			urxBackslashV: // Any line ending code point, with optional negation
			if currentLen == 0 {
				s := uset.New()
				s.AddRuneRange(0x0a, 0x0d) // add range
				s.AddRune(0x85)
				s.AddRuneRange(0x2028, 0x2029)
				if op.value() != 0 {
					// Complement option applies to URX_BACKSLASH_V only.
					s.Complement()
				}
				c.out.initialChars.AddAll(s)
				numInitialStrings += 2
			}
			currentLen = safeIncrement(currentLen, 1)
			atStart = false

		case urcOnecharI:
			// Case Insensitive Single Character.
			if currentLen == 0 {
				ch := op.value32()
				if uprops.HasBinaryProperty(ch, uprops.UCharCaseSensitive) {
					starters := uset.New()
					starters.AddRuneRange(ch, ch)
					starters.CloseOver(uset.CaseInsensitive)
					// findCaseInsensitiveStarters(c, &starters);
					//   For ONECHAR_I, no need to worry about text chars that expand on folding into
					//   strings. The expanded folding can't match the pattern.
					c.out.initialChars.AddAll(starters)
				} else {
					// Char has no case variants.  Just add it as-is to the
					//   set of possible starting chars.
					c.out.initialChars.AddRune(ch)
				}
				numInitialStrings += 2
			}
			currentLen = safeIncrement(currentLen, 1)
			atStart = false

		case urxBackslashX, // Grahpeme Cluster.  Minimum is 1, max unbounded.
			urxDotanyAll, // . matches one or two.
			urxDotany,
			urxDotanyUnix:
			if currentLen == 0 {
				// These constructs are all bad news when they appear at the start
				//   of a match.  Any character can begin the match.
				c.out.initialChars.Clear()
				c.out.initialChars.Complement()
				numInitialStrings += 2
			}
			currentLen = safeIncrement(currentLen, 1)
			atStart = false

		case urxJmpx:
			loc++ // Except for extra operand on URX_JMPX, same as URX_JMP.
			fallthrough

		case urxJmp:
			jmpDest := op.value()
			if jmpDest < loc {
				// Loop of some kind.  Can safely ignore, the worst that will happen
				//  is that we understate the true minimum length
				currentLen = forwardedLength[loc+1]
			} else {
				// Forward jump.  Propagate the current min length to the target loc of the jump.
				if forwardedLength[jmpDest] > currentLen {
					forwardedLength[jmpDest] = currentLen
				}
			}
			atStart = false

		case urxJmpSav,
			urxJmpSavX:
			// Combo of state save to the next loc, + jmp backwards.
			//   Net effect on min. length computation is nothing.
			atStart = false

		case urxBacktrack:
			// Fails are kind of like a branch, except that the min length was
			//   propagated already, by the state save.
			currentLen = forwardedLength[loc+1]
			atStart = false

		case urxStateSave:
			// State Save, for forward jumps, propagate the current minimum.
			//             of the state save.
			jmpDest := op.value()
			if jmpDest > loc {
				if currentLen < forwardedLength[jmpDest] {
					forwardedLength[jmpDest] = (currentLen)
				}
			}
			atStart = false

		case urxString:
			loc++
			stringLenOp := c.out.compiledPat[loc]
			stringLen := stringLenOp.value()
			if currentLen == 0 {
				// Add the starting character of this string to the set of possible starting
				//   characters for this pattern.
				stringStartIdx := op.value()
				ch := c.out.literalText[stringStartIdx]
				c.out.initialChars.AddRune(ch)

				// Remember this string.  After the entire pattern has been checked,
				//  if nothing else is identified that can start a match, we'll use it.
				numInitialStrings++
				c.out.initialStringIdx = stringStartIdx
				c.out.initialStringLen = stringLen
			}

			currentLen = safeIncrement(currentLen, stringLen)
			atStart = false

		case urxStringI:
			// Case-insensitive string.  Unlike exact-match strings, we won't
			//   attempt a string search for possible match positions.  But we
			//   do update the set of possible starting characters.
			loc++
			stringLenOp := c.out.compiledPat[loc]
			stringLen := stringLenOp.value()
			if currentLen == 0 {
				// Add the starting character of this string to the set of possible starting
				//   characters for this pattern.
				stringStartIdx := op.value()
				ch := c.out.literalText[stringStartIdx]
				s := uset.New()
				c.findCaseInsensitiveStarters(ch, s)
				c.out.initialChars.AddAll(s)
				numInitialStrings += 2 // Matching on an initial string not possible.
			}
			currentLen = safeIncrement(currentLen, stringLen)
			atStart = false

		case urxCtrInit,
			urxCtrInitNg:
			// Loop Init Ops.  These don't change the min length, but they are 4 word ops
			//   so location must be updated accordingly.
			// Loop Init Ops.
			//   If the min loop count == 0
			//      move loc forwards to the end of the loop, skipping over the body.
			//   If the min count is > 0,
			//      continue normal processing of the body of the loop.
			loopEndLoc := c.out.compiledPat[loc+1].value()
			minLoopCount := int(c.out.compiledPat[loc+2])
			if minLoopCount == 0 {
				// Min Loop Count of 0, treat like a forward branch and
				//   move the current minimum length up to the target
				//   (end of loop) location.
				if forwardedLength[loopEndLoc] > currentLen {
					forwardedLength[loopEndLoc] = currentLen
				}
			}
			loc += 3 // Skips over operands of CTR_INIT
			atStart = false

		case utxCtrLoop,
			urxCtrLoopNg:
			// Loop ops.
			//  The jump is conditional, backwards only.
			atStart = false

		case urxLoopC:
			// More loop ops.  These state-save to themselves.
			//   don't change the minimum match
			atStart = false

		case urxLaStart,
			urxLbStart:
			// Look-around.  Scan forward until the matching look-ahead end,
			//   without processing the look-around block.  This is overly pessimistic.

			// Keep track of the nesting depth of look-around blocks.  Boilerplate code for
			//   lookahead contains two LA_END instructions, so count goes up by two
			//   for each LA_START.
			var depth int
			if opType == urxLaStart {
				depth = 2
			} else {
				depth = 1
			}
			for {
				loc++
				op = c.out.compiledPat[loc]
				if op.typ() == urxLaStart {
					depth += 2
				}
				if op.typ() == urxLbStart {
					depth++
				}
				if op.typ() == urxLaEnd || op.typ() == urxLbnEnd {
					depth--
					if depth == 0 {
						break
					}
				}
				if op.typ() == urxStateSave {
					// Need this because neg lookahead blocks will FAIL to outside
					//   of the block.
					jmpDest := op.value()
					if jmpDest > loc {
						if currentLen < forwardedLength[jmpDest] {
							forwardedLength[jmpDest] = (currentLen)
						}
					}
				}
			}

		case urxLaEnd,
			urxLbCont,
			urxLbEnd,
			urxLbnCount,
			urxLbnEnd:
			panic("should be consumed in URX_LA_START")

		default:
			panic("unreachable")
		}
	}

	// Sort out what we should check for when looking for candidate match start positions.
	// In order of preference,
	//     1.   Start of input text buffer.
	//     2.   A literal string.
	//     3.   Start of line in multi-line mode.
	//     4.   A single literal character.
	//     5.   A character from a set of characters.
	//
	if c.out.startType == startStart {
		// Match only at the start of an input text string.
		//    start type is already set.  We're done.
	} else if numInitialStrings == 1 && c.out.minMatchLen > 0 {
		// Match beginning only with a literal string.
		ch := c.out.literalText[c.out.initialStringIdx]
		c.out.startType = startString
		c.out.initialChar = ch
	} else if c.out.startType == startLine {
		// Match at start of line in Multi-Line mode.
		// Nothing to do here; everything is already set.
	} else if c.out.minMatchLen == 0 {
		// Zero length match possible.  We could start anywhere.
		c.out.startType = startNoInfo
	} else if c.out.initialChars.Len() == 1 {
		// All matches begin with the same char.
		c.out.startType = startChar
		c.out.initialChar = c.out.initialChars.RuneAt(0)
	} else if !c.out.initialChars.ContainsRuneRange(0, 0x10ffff) && c.out.minMatchLen > 0 {
		// Matches start with a set of character smaller than the set of all chars.
		c.out.startType = startSet
	} else {
		// Matches can start with anything
		c.out.startType = startNoInfo
	}
}

func (c *compiler) appendOp(typ opcode, arg int) {
	c.appendIns(c.buildOp(typ, arg))
}

func (c *compiler) appendIns(ins instruction) {
	if c.err != nil {
		return
	}
	c.out.compiledPat = append(c.out.compiledPat, ins)
}

func (c *compiler) buildOp(typ opcode, val int) instruction {
	if c.err != nil {
		return 0
	}
	if val > 0x00ffffff {
		panic("bad argument to buildOp")
	}
	if val < 0 {
		if !(typ == urxReservedOpN || typ == urxReservedOp) {
			panic("bad value to buildOp")
		}
		typ = urxReservedOpN
	}
	return instruction(int32(typ)<<24 | int32(val))
}

func (c *compiler) handleCloseParen() {
	if len(c.parenStack) == 0 {
		c.error(MismatchedParen)
		return
	}

	c.fixLiterals(false)

	var patIdx int
	var patOp instruction

	for {
		patIdx, c.parenStack = stackPop(c.parenStack)
		if patIdx < 0 {
			break
		}

		patOp = c.out.compiledPat[patIdx]
		if patOp.value() != 0 {
			panic("branch target for JMP should not be set")
		}
		patOp |= instruction(len(c.out.compiledPat))
		c.out.compiledPat[patIdx] = patOp
		c.matchOpenParen = patIdx
	}

	var modeFlags int
	modeFlags, c.parenStack = stackPop(c.parenStack)
	if modeFlags >= 0 {
		panic("modeFlags in paren stack was not negated")
	}

	c.modeFlags = RegexpFlag(modeFlags)

	switch patIdx {
	case parenPlain, parenFlags:
	// No additional fixups required.
	//   (Grouping-only parentheses)
	case parenCapturing:
		// Capturing Parentheses.
		//   Insert a End Capture op into the pattern.
		//   The frame offset of the variables for this cg is obtained from the
		//       start capture op and put it into the end-capture op.

		captureOp := c.out.compiledPat[c.matchOpenParen+1]
		if captureOp.typ() != urxStartCapture {
			panic("bad type in capture op (expected URX_START_CAPTURE)")
		}
		frameVarLocation := captureOp.value()
		c.appendOp(urxEndCapture, frameVarLocation)

	case parenAtomic:
		// Atomic Parenthesis.
		//   Insert a LD_SP operation to restore the state stack to the position
		//   it was when the atomic parens were entered.
		stoOp := c.out.compiledPat[c.matchOpenParen+1]
		if stoOp.typ() != urxStoSp {
			panic("bad type in capture op (expected URX_STO_SP)")
		}
		stoLoc := stoOp.value()
		c.appendOp(urxLdSp, stoLoc)

	case parenLookahead:
		startOp := c.out.compiledPat[c.matchOpenParen-5]
		if startOp.typ() != urxLaStart {
			panic("bad type in capture op (expected URX_LA_START)")
		}
		dataLoc := startOp.value()
		c.appendOp(urxLaEnd, dataLoc)

	case parenNegLookahead:
		startOp := c.out.compiledPat[c.matchOpenParen-1]
		if startOp.typ() != urxLaStart {
			panic("bad type in capture op (expected URX_LA_START)")
		}
		dataLoc := startOp.value()
		c.appendOp(urxLaEnd, dataLoc)
		c.appendOp(urxBacktrack, 0)
		c.appendOp(urxLaEnd, dataLoc)

		// Patch the URX_SAVE near the top of the block.
		// The destination of the SAVE is the final LA_END that was just added.
		saveOp := c.out.compiledPat[c.matchOpenParen]
		if saveOp.typ() != urxStateSave {
			panic("bad type in capture op (expected URX_STATE_SAVE)")
		}
		saveOp = c.buildOp(urxStateSave, len(c.out.compiledPat)-1)
		c.out.compiledPat[c.matchOpenParen] = saveOp

	case parenLookBehind:
		startOp := c.out.compiledPat[c.matchOpenParen-4]
		if startOp.typ() != urxLbStart {
			panic("bad type in capture op (expected URX_LB_START)")
		}
		dataLoc := startOp.value()
		c.appendOp(urxLbEnd, dataLoc)
		c.appendOp(urxLaEnd, dataLoc)

		// Determine the min and max bounds for the length of the
		//  string that the pattern can match.
		//  An unbounded upper limit is an error.
		patEnd := len(c.out.compiledPat) - 1
		minML := c.minMatchLength(c.matchOpenParen, patEnd)
		maxML := c.maxMatchLength(c.matchOpenParen, patEnd)

		if maxML == math.MaxInt32 {
			c.error(LookBehindLimit)
			break
		}
		if minML == math.MaxInt32 {
			// This condition happens when no match is possible, such as with a
			// [set] expression containing no elements.
			// In principle, the generated code to evaluate the expression could be deleted,
			// but it's probably not worth the complication.
			minML = 0
		}

		c.out.compiledPat[c.matchOpenParen-2] = instruction(minML)
		c.out.compiledPat[c.matchOpenParen-1] = instruction(maxML)

	case parenLookBehindN:
		startOp := c.out.compiledPat[c.matchOpenParen-5]
		if startOp.typ() != urxLbStart {
			panic("bad type in capture op (expected URX_LB_START)")
		}
		dataLoc := startOp.value()
		c.appendOp(urxLbnEnd, dataLoc)

		// Determine the min and max bounds for the length of the
		//  string that the pattern can match.
		//  An unbounded upper limit is an error.
		patEnd := len(c.out.compiledPat) - 1
		minML := c.minMatchLength(c.matchOpenParen, patEnd)
		maxML := c.maxMatchLength(c.matchOpenParen, patEnd)

		if instruction(maxML).typ() != 0 {
			c.error(LookBehindLimit)
			break
		}
		if maxML == math.MaxInt32 {
			c.error(LookBehindLimit)
			break
		}
		if minML == math.MaxInt32 {
			// This condition happens when no match is possible, such as with a
			// [set] expression containing no elements.
			// In principle, the generated code to evaluate the expression could be deleted,
			// but it's probably not worth the complication.
			minML = 0
		}

		c.out.compiledPat[c.matchOpenParen-3] = instruction(minML)
		c.out.compiledPat[c.matchOpenParen-2] = instruction(maxML)

		op := c.buildOp(urxRelocOprnd, len(c.out.compiledPat))
		c.out.compiledPat[c.matchOpenParen-1] = op

	default:
		panic("unexpected opcode in parenStack")
	}

	c.matchCloseParen = len(c.out.compiledPat)
}

func (c *compiler) fixLiterals(split bool) {
	if len(c.literalChars) == 0 {
		return
	}

	lastCodePoint := c.literalChars[len(c.literalChars)-1]

	// Split:  We need to  ensure that the last item in the compiled pattern
	//     refers only to the last literal scanned in the pattern, so that
	//     quantifiers (*, +, etc.) affect only it, and not a longer string.
	//     Split before case folding for case insensitive matches.
	if split {
		c.literalChars = c.literalChars[:len(c.literalChars)-1]
		c.fixLiterals(false)

		c.literalChar(lastCodePoint)
		c.fixLiterals(false)
		return
	}

	if c.modeFlags&CaseInsensitive != 0 {
		c.literalChars = ucase.FoldRunes(c.literalChars)
		lastCodePoint = c.literalChars[len(c.literalChars)-1]
	}

	if len(c.literalChars) == 1 {
		if c.modeFlags&CaseInsensitive != 0 && uprops.HasBinaryProperty(lastCodePoint, uprops.UCharCaseSensitive) {
			c.appendOp(urcOnecharI, int(lastCodePoint))
		} else {
			c.appendOp(urxOnechar, int(lastCodePoint))
		}
	} else {
		if len(c.literalChars) > 0x00ffffff || len(c.out.literalText) > 0x00ffffff {
			c.error(PatternTooBig)
		}
		if c.modeFlags&CaseInsensitive != 0 {
			c.appendOp(urxStringI, len(c.out.literalText))
		} else {
			c.appendOp(urxString, len(c.out.literalText))
		}
		c.appendOp(urxStringLen, len(c.literalChars))
		c.out.literalText = append(c.out.literalText, c.literalChars...)
	}

	c.literalChars = c.literalChars[:0]
}

func (c *compiler) literalChar(point rune) {
	c.literalChars = append(c.literalChars, point)
}

func (c *compiler) allocateData(size int) int {
	if c.err != nil {
		return 0
	}
	if size <= 0 || size > 0x100 || c.out.dataSize < 0 {
		c.error(InternalError)
		return 0
	}

	dataIndex := c.out.dataSize
	c.out.dataSize += size
	if c.out.dataSize >= 0x00fffff0 {
		c.error(InternalError)
	}
	return dataIndex
}

func (c *compiler) allocateStackData(size int) int {
	if c.err != nil {
		return 0
	}
	if size <= 0 || size > 0x100 || c.out.frameSize < 0 {
		c.error(InternalError)
		return 0
	}
	dataIndex := c.out.frameSize
	c.out.frameSize += size
	if c.out.frameSize >= 0x00fffff0 {
		c.error(InternalError)
	}
	return dataIndex
}

func (c *compiler) insertOp(where int) {
	if where < 0 || where >= len(c.out.compiledPat) {
		panic("insertOp: out of bounds")
	}

	nop := c.buildOp(urxNop, 0)
	c.out.compiledPat = slices.Insert(c.out.compiledPat, where, nop)

	// Walk through the pattern, looking for any ops with targets that
	//  were moved down by the insert.  Fix them.
	for loc, op := range c.out.compiledPat {
		switch op.typ() {
		case urxJmp, urxJmpx, urxStateSave, utxCtrLoop, urxCtrLoopNg, urxJmpSav, urxJmpSavX, urxRelocOprnd:
			if op.value() > where {
				op = c.buildOp(op.typ(), op.value()+1)
				c.out.compiledPat[loc] = op
			}
		}
	}

	// Now fix up the parentheses stack.  All positive values in it are locations in
	//  the compiled pattern.   (Negative values are frame boundaries, and don't need fixing.)
	for loc, x := range c.parenStack {
		if x > where {
			c.parenStack[loc] = x + 1
		}
	}

	if c.matchCloseParen > where {
		c.matchCloseParen++
	}
	if c.matchOpenParen > where {
		c.matchOpenParen++
	}
}

func (c *compiler) blockTopLoc(reserve bool) int {
	var loc int
	c.fixLiterals(true)

	if len(c.out.compiledPat) == c.matchCloseParen {
		// The item just processed is a parenthesized block.
		loc = c.matchOpenParen
	} else {
		// Item just compiled is a single thing, a ".", or a single char, a string or a set reference.
		// No slot for STATE_SAVE was pre-reserved in the compiled code.
		// We need to make space now.
		loc = len(c.out.compiledPat) - 1
		op := c.out.compiledPat[loc]
		if op.typ() == urxStringLen {
			// Strings take two opcode, we want the position of the first one.
			// We can have a string at this point if a single character case-folded to two.
			loc--
		}
		if reserve {
			nop := c.buildOp(urxNop, 0)
			c.out.compiledPat = slices.Insert(c.out.compiledPat, loc, nop)
		}
	}
	return loc
}

func (c *compiler) compileInlineInterval() bool {
	if c.intervalUpper > 10 || c.intervalUpper < c.intervalLow {
		return false
	}

	topOfBlock := c.blockTopLoc(false)
	if c.intervalUpper == 0 {
		// Pathological case.  Attempt no matches, as if the block doesn't exist.
		// Discard the generated code for the block.
		// If the block included parens, discard the info pertaining to them as well.
		c.out.compiledPat = c.out.compiledPat[:topOfBlock]
		if c.matchOpenParen >= topOfBlock {
			c.matchOpenParen = -1
		}
		if c.matchCloseParen >= topOfBlock {
			c.matchCloseParen = -1
		}
		return true
	}

	if topOfBlock != len(c.out.compiledPat)-1 && c.intervalUpper != 1 {
		// The thing being repeated is not a single op, but some
		//   more complex block.  Do it as a loop, not inlines.
		//   Note that things "repeated" a max of once are handled as inline, because
		//     the one copy of the code already generated is just fine.
		return false
	}

	// Pick up the opcode that is to be repeated
	//
	op := c.out.compiledPat[topOfBlock]

	// Compute the pattern location where the inline sequence
	//   will end, and set up the state save op that will be needed.
	//
	endOfSequenceLoc := len(c.out.compiledPat) - 1 + c.intervalUpper + (c.intervalUpper - c.intervalLow)

	saveOp := c.buildOp(urxStateSave, endOfSequenceLoc)
	if c.intervalLow == 0 {
		c.insertOp(topOfBlock)
		c.out.compiledPat[topOfBlock] = saveOp
	}

	//  Loop, emitting the op for the thing being repeated each time.
	//    Loop starts at 1 because one instance of the op already exists in the pattern,
	//    it was put there when it was originally encountered.
	for i := 1; i < c.intervalUpper; i++ {
		if i >= c.intervalLow {
			c.appendIns(saveOp)
		}
		c.appendIns(op)
	}
	return true
}

func (c *compiler) compileInterval(init opcode, loop opcode) {
	// The CTR_INIT op at the top of the block with the {n,m} quantifier takes
	//   four slots in the compiled code.  Reserve them.
	topOfBlock := c.blockTopLoc(true)
	c.insertOp(topOfBlock)
	c.insertOp(topOfBlock)
	c.insertOp(topOfBlock)

	// The operands for the CTR_INIT opcode include the index in the matcher data
	//   of the counter.  Allocate it now. There are two data items
	//        counterLoc   -->  Loop counter
	//               +1    -->  Input index (for breaking non-progressing loops)
	//                          (Only present if unbounded upper limit on loop)
	var dataSize int
	if c.intervalUpper < 0 {
		dataSize = 2
	} else {
		dataSize = 1
	}
	counterLoc := c.allocateStackData(dataSize)

	op := c.buildOp(init, counterLoc)
	c.out.compiledPat[topOfBlock] = op

	// The second operand of CTR_INIT is the location following the end of the loop.
	//   Must put in as a URX_RELOC_OPRND so that the value will be adjusted if the
	//   compilation of something later on causes the code to grow and the target
	//   position to move.
	loopEnd := len(c.out.compiledPat)
	op = c.buildOp(urxRelocOprnd, loopEnd)
	c.out.compiledPat[topOfBlock+1] = op

	// Followed by the min and max counts.
	c.out.compiledPat[topOfBlock+2] = instruction(c.intervalLow)
	c.out.compiledPat[topOfBlock+3] = instruction(c.intervalUpper)

	// Append the CTR_LOOP op.  The operand is the location of the CTR_INIT op.
	//   Goes at end of the block being looped over, so just append to the code so far.
	c.appendOp(loop, topOfBlock)

	if (c.intervalLow&0xff000000) != 0 || (c.intervalUpper > 0 && (c.intervalUpper&0xff000000) != 0) {
		c.error(NumberTooBig)
	}

	if c.intervalLow > c.intervalUpper && c.intervalUpper != -1 {
		c.error(MaxLtMin)
	}
}

func (c *compiler) scanNamedChar() rune {
	c.nextChar(&c.c)
	if c.c.char != chLBrace {
		c.error(PropertySyntax)
		return 0
	}

	var charName []rune
	for {
		c.nextChar(&c.c)
		if c.c.char == chRBrace {
			break
		}
		if c.c.char == -1 {
			c.error(PropertySyntax)
			return 0
		}
		charName = append(charName, c.c.char)
	}

	if !isInvariantUString(charName) {
		// All Unicode character names have only invariant characters.
		// The API to get a character, given a name, accepts only char *, forcing us to convert,
		//   which requires this error check
		c.error(PropertySyntax)
		return 0
	}

	theChar := unames.CharForName(unames.UnicodeCharName, string(charName))
	if c.err != nil {
		c.error(PropertySyntax)
	}

	c.nextChar(&c.c) // Continue overall regex pattern processing with char after the '}'
	return theChar
}

func isInvariantUString(name []rune) bool {
	for _, c := range name {
		/*
		 * no assertions here because these functions are legitimately called
		 * for strings with variant characters
		 */
		if !ucharIsInvariant(c) {
			return false /* found a variant char */
		}
	}
	return true
}

var invariantChars = [...]uint32{
	0xfffffbff, /* 00..1f but not 0a */
	0xffffffe5, /* 20..3f but not 21 23 24 */
	0x87fffffe, /* 40..5f but not 40 5b..5e */
	0x87fffffe, /* 60..7f but not 60 7b..7e */
}

func ucharIsInvariant(c rune) bool {
	return c <= 0x7f && (invariantChars[(c)>>5]&(uint32(1)<<(c&0x1f))) != 0
}

func (c *compiler) setPushOp(op setOperation) {
	c.setEval(op)
	c.setOpStack = append(c.setOpStack, op)
	c.setStack = append(c.setStack, uset.New())
}

func (c *compiler) setEval(nextOp setOperation) {
	var rightOperand *uset.UnicodeSet
	var leftOperand *uset.UnicodeSet

	for {
		pendingSetOp := c.setOpStack[len(c.setOpStack)-1]
		if (pendingSetOp & 0xffff0000) < (nextOp & 0xffff0000) {
			break
		}

		c.setOpStack = c.setOpStack[:len(c.setOpStack)-1]
		rightOperand = c.setStack[len(c.setStack)-1]

		switch pendingSetOp {
		case setNegation:
			rightOperand.Complement()

		case setCaseClose:
			rightOperand.CloseOver(uset.CaseInsensitive)

		case setDifference1, setDifference2:
			c.setStack = c.setStack[:len(c.setStack)-1]
			leftOperand = c.setStack[len(c.setStack)-1]
			leftOperand.RemoveAll(rightOperand)

		case setIntersection1, setIntersection2:
			c.setStack = c.setStack[:len(c.setStack)-1]
			leftOperand = c.setStack[len(c.setStack)-1]
			leftOperand.RetainAll(rightOperand)

		case setUnion:
			c.setStack = c.setStack[:len(c.setStack)-1]
			leftOperand = c.setStack[len(c.setStack)-1]
			leftOperand.AddAll(rightOperand)

		default:
			panic("unreachable")
		}
	}
}

func safeIncrement(val int32, delta int) int32 {
	if delta <= math.MaxInt32 && math.MaxInt32-val > int32(delta) {
		return val + int32(delta)
	}
	return math.MaxInt32
}

func (c *compiler) minMatchLength(start, end int) int32 {
	if c.err != nil {
		return 0
	}

	var loc int
	var currentLen int32

	// forwardedLength is a vector holding minimum-match-length values that
	//   are propagated forward in the pattern by JMP or STATE_SAVE operations.
	//   It must be one longer than the pattern being checked because some  ops
	//   will jmp to a end-of-block+1 location from within a block, and we must
	//   count those when checking the block.
	forwardedLength := make([]int32, end+2)
	for i := range forwardedLength {
		forwardedLength[i] = math.MaxInt32
	}

	for loc = start; loc <= end; loc++ {
		op := c.out.compiledPat[loc]
		opType := op.typ()

		// The loop is advancing linearly through the pattern.
		// If the op we are now at was the destination of a branch in the pattern,
		// and that path has a shorter minimum length than the current accumulated value,
		// replace the current accumulated value.
		//   no-match-possible cases.
		if forwardedLength[loc] < currentLen {
			currentLen = forwardedLength[loc]
		}

		switch opType {
		// Ops that don't change the total length matched
		case urxReservedOp,
			urxEnd,
			urxStringLen,
			urxNop,
			urxStartCapture,
			urxEndCapture,
			urxBackslashB,
			urxBackslashBu,
			urxBackslashG,
			urxBackslashZ,
			urxCaret,
			urxDollar,
			urxDollarM,
			urxDollarD,
			urxDollarMd,
			urxRelocOprnd,
			urxStoInpLoc,
			urxCaretM,
			urxCaretMUnix,
			urxBackref, // BackRef.  Must assume that it might be a zero length match
			urxBackrefI,
			urxStoSp, // Setup for atomic or possessive blocks.  Doesn't change what can match.
			urxLdSp,
			urxJmpSav,
			urxJmpSavX:
			// no-op

			// Ops that match a minimum of one character (one or two 16 bit code units.)
			//
		case urxOnechar,
			urxStaticSetref,
			urxStatSetrefN,
			urxSetref,
			urxBackslashD,
			urxBackslashH,
			urxBackslashR,
			urxBackslashV,
			urcOnecharI,
			urxBackslashX, // Grahpeme Cluster.  Minimum is 1, max unbounded.
			urxDotanyAll,  // . matches one or two.
			urxDotany,
			urxDotanyUnix:
			currentLen = safeIncrement(currentLen, 1)

		case urxJmpx:
			loc++ // URX_JMPX has an extra operand, ignored here, otherwise processed identically to URX_JMP.
			fallthrough

		case urxJmp:
			jmpDest := op.value()
			if jmpDest < loc {
				// Loop of some kind.  Can safely ignore, the worst that will happen
				//  is that we understate the true minimum length
				currentLen = forwardedLength[loc+1]
			} else {
				// Forward jump.  Propagate the current min length to the target loc of the jump.
				if forwardedLength[jmpDest] > currentLen {
					forwardedLength[jmpDest] = currentLen
				}
			}

		case urxBacktrack:
			// Back-tracks are kind of like a branch, except that the min length was
			//   propagated already, by the state save.
			currentLen = forwardedLength[loc+1]

		case urxStateSave:
			// State Save, for forward jumps, propagate the current minimum.
			//             of the state save.
			jmpDest := op.value()
			if jmpDest > loc {
				if currentLen < forwardedLength[jmpDest] {
					forwardedLength[jmpDest] = currentLen
				}
			}

		case urxString:
			loc++
			stringLenOp := c.out.compiledPat[loc]
			currentLen = safeIncrement(currentLen, stringLenOp.value())

		case urxStringI:
			loc++
			// TODO: with full case folding, matching input text may be shorter than
			//       the string we have here.  More smarts could put some bounds on it.
			//       Assume a min length of one for now.  A min length of zero causes
			//        optimization failures for a pattern like "string"+
			// currentLen += URX_VAL(stringLenOp);
			currentLen = safeIncrement(currentLen, 1)

		case urxCtrInit, urxCtrInitNg:
			// Loop Init Ops.
			//   If the min loop count == 0
			//      move loc forwards to the end of the loop, skipping over the body.
			//   If the min count is > 0,
			//      continue normal processing of the body of the loop.
			loopEndOp := c.out.compiledPat[loc+1]
			loopEndLoc := loopEndOp.value()
			minLoopCount := c.out.compiledPat[loc+2]
			if minLoopCount == 0 {
				loc = loopEndLoc
			} else {
				loc += 3 // Skips over operands of CTR_INIT
			}

		case utxCtrLoop, urxCtrLoopNg:
			// Loop ops. The jump is conditional, backwards only.

		case urxLoopSrI, urxLoopDotI, urxLoopC:
			// More loop ops.  These state-save to themselves. don't change the minimum match - could match nothing at all.

		case urxLaStart, urxLbStart:
			// Look-around.  Scan forward until the matching look-ahead end,
			//   without processing the look-around block.  This is overly pessimistic for look-ahead,
			//   it assumes that the look-ahead match might be zero-length.
			//   TODO:  Positive lookahead could recursively do the block, then continue
			//          with the longer of the block or the value coming in.  Ticket 6060
			var depth int32
			if opType == urxLaStart {
				depth = 2
			} else {
				depth = 1
			}

			for {
				loc++
				op = c.out.compiledPat[loc]
				if op.typ() == urxLaStart {
					// The boilerplate for look-ahead includes two LA_END insturctions,
					//    Depth will be decremented by each one when it is seen.
					depth += 2
				}
				if op.typ() == urxLbStart {
					depth++
				}
				if op.typ() == urxLaEnd {
					depth--
					if depth == 0 {
						break
					}
				}
				if op.typ() == urxLbnEnd {
					depth--
					if depth == 0 {
						break
					}
				}
				if op.typ() == urxStateSave {
					// Need this because neg lookahead blocks will FAIL to outside of the block.
					jmpDest := op.value()
					if jmpDest > loc {
						if currentLen < forwardedLength[jmpDest] {
							forwardedLength[jmpDest] = currentLen
						}
					}
				}
			}

		case urxLaEnd, urxLbCont, urxLbEnd, urxLbnCount, urxLbnEnd:
			// Only come here if the matching URX_LA_START or URX_LB_START was not in the
			//   range being sized, which happens when measuring size of look-behind blocks.

		default:
			panic("unreachable")
		}
	}

	// We have finished walking through the ops.  Check whether some forward jump
	//   propagated a shorter length to location end+1.
	if forwardedLength[end+1] < currentLen {
		currentLen = forwardedLength[end+1]
	}

	return currentLen
}

func (c *compiler) maxMatchLength(start, end int) int32 {
	if c.err != nil {
		return 0
	}
	var loc int
	var currentLen int32

	forwardedLength := make([]int32, end+1)

	for loc = start; loc <= end; loc++ {
		op := c.out.compiledPat[loc]
		opType := op.typ()

		// The loop is advancing linearly through the pattern.
		// If the op we are now at was the destination of a branch in the pattern,
		// and that path has a longer maximum length than the current accumulated value,
		// replace the current accumulated value.
		if forwardedLength[loc] > currentLen {
			currentLen = forwardedLength[loc]
		}

		switch opType {
		// Ops that don't change the total length matched
		case urxReservedOp,
			urxEnd,
			urxStringLen,
			urxNop,
			urxStartCapture,
			urxEndCapture,
			urxBackslashB,
			urxBackslashBu,
			urxBackslashG,
			urxBackslashZ,
			urxCaret,
			urxDollar,
			urxDollarM,
			urxDollarD,
			urxDollarMd,
			urxRelocOprnd,
			urxStoInpLoc,
			urxCaretM,
			urxCaretMUnix,
			urxStoSp, // Setup for atomic or possessive blocks.  Doesn't change what can match.
			urxLdSp,
			urxLbEnd,
			urxLbCont,
			urxLbnCount,
			urxLbnEnd:
		// no-op

		// Ops that increase that cause an unbounded increase in the length
		//   of a matched string, or that increase it a hard to characterize way.
		//   Call the max length unbounded, and stop further checking.
		case urxBackref, // BackRef.  Must assume that it might be a zero length match
			urxBackrefI,
			urxBackslashX: // Grahpeme Cluster.  Minimum is 1, max unbounded.
			currentLen = math.MaxInt32

			// Ops that match a max of one character (possibly two 16 bit code units.)
			//
		case urxStaticSetref,
			urxStatSetrefN,
			urxSetref,
			urxBackslashD,
			urxBackslashH,
			urxBackslashR,
			urxBackslashV,
			urcOnecharI,
			urxDotanyAll,
			urxDotany,
			urxDotanyUnix:
			currentLen = safeIncrement(currentLen, 2)

			// Single literal character.  Increase current max length by one or two,
			//       depending on whether the char is in the supplementary range.
		case urxOnechar:
			currentLen = safeIncrement(currentLen, 1)
			if op.value() > 0x10000 {
				currentLen = safeIncrement(currentLen, 1)
			}

			// Jumps.
			//
		case urxJmp, urxJmpx, urxJmpSav, urxJmpSavX:
			jmpDest := op.value()
			if jmpDest < loc {
				// Loop of some kind.  Max match length is unbounded.
				currentLen = math.MaxInt32
			} else {
				// Forward jump.  Propagate the current min length to the target loc of the jump.
				if forwardedLength[jmpDest] < currentLen {
					forwardedLength[jmpDest] = currentLen
				}
				currentLen = 0
			}

		case urxBacktrack:
			// back-tracks are kind of like a branch, except that the max length was
			//   propagated already, by the state save.
			currentLen = forwardedLength[loc+1]

		case urxStateSave:
			// State Save, for forward jumps, propagate the current minimum.
			//               of the state save.
			//             For backwards jumps, they create a loop, maximum
			//               match length is unbounded.
			jmpDest := op.value()
			if jmpDest > loc {
				if currentLen > forwardedLength[jmpDest] {
					forwardedLength[jmpDest] = currentLen
				}
			} else {
				currentLen = math.MaxInt32
			}

		case urxString:
			loc++
			stringLenOp := c.out.compiledPat[loc]
			currentLen = safeIncrement(currentLen, stringLenOp.value())

		case urxStringI:
			// TODO:  This code assumes that any user string that matches will be no longer
			//        than our compiled string, with case insensitive matching.
			//        Our compiled string has been case-folded already.
			//
			//        Any matching user string will have no more code points than our
			//        compiled (folded) string.  Folding may add code points, but
			//        not remove them.
			//
			//        There is a potential problem if a supplemental code point
			//        case-folds to a BMP code point.  In this case our compiled string
			//        could be shorter (in code units) than a matching user string.
			//
			//        At this time (Unicode 6.1) there are no such characters, and this case
			//        is not being handled.  A test, intltest regex/Bug9283, will fail if
			//        any problematic characters are added to Unicode.
			//
			//        If this happens, we can make a set of the BMP chars that the
			//        troublesome supplementals fold to, scan our string, and bump the
			//        currentLen one extra for each that is found.
			//
			loc++
			stringLenOp := c.out.compiledPat[loc]
			currentLen = safeIncrement(currentLen, stringLenOp.value())

		case urxCtrInit, urxCtrInitNg:
			// For Loops, recursively call this function on the pattern for the loop body,
			//   then multiply the result by the maximum loop count.
			loopEndLoc := c.out.compiledPat[loc+1].value()
			if loopEndLoc == loc+4 {
				// Loop has an empty body. No affect on max match length.
				// Continue processing with code after the loop end.
				loc = loopEndLoc
				break
			}

			maxLoopCount := int(c.out.compiledPat[loc+3])
			if maxLoopCount == -1 {
				// Unbounded Loop. No upper bound on match length.
				currentLen = math.MaxInt32
				break
			}

			blockLen := c.maxMatchLength(loc+4, loopEndLoc-1) // Recursive call.
			updatedLen := int(currentLen) + int(blockLen)*maxLoopCount
			if updatedLen >= math.MaxInt32 {
				currentLen = math.MaxInt32
				break
			}
			currentLen = int32(updatedLen)
			loc = loopEndLoc

		case utxCtrLoop, urxCtrLoopNg:
			panic("should not encounter this opcode")

		case urxLoopSrI, urxLoopDotI, urxLoopC:
			// For anything to do with loops, make the match length unbounded.
			currentLen = math.MaxInt32

		case urxLaStart, urxLaEnd:
			// Look-ahead.  Just ignore, treat the look-ahead block as if
			// it were normal pattern.  Gives a too-long match length,
			//  but good enough for now.

		case urxLbStart:
			// Look-behind.  Scan forward until the matching look-around end,
			//   without processing the look-behind block.
			dataLoc := op.value()
			for loc = loc + 1; loc <= end; loc++ {
				op = c.out.compiledPat[loc]
				if (op.typ() == urxLaEnd || op.typ() == urxLbnEnd) && (op.value() == dataLoc) {
					break
				}
			}

		default:
			panic("unreachable")
		}

		if currentLen == math.MaxInt32 {
			//  The maximum length is unbounded.
			//  Stop further processing of the pattern.
			break
		}
	}

	return currentLen
}

// Machine Generated below.
// It may need updating with new versions of Unicode.
// Intltest test RegexTest::TestCaseInsensitiveStarters will fail if an update is needed.
// The update tool is here:
// svn+ssh://source.icu-project.org/repos/icu/tools/trunk/unicode/c/genregexcasing

// Machine Generated Data. Do not hand edit.
var reCaseFixCodePoints = [...]rune{
	0x61, 0x66, 0x68, 0x69, 0x6a, 0x73, 0x74, 0x77, 0x79, 0x2bc,
	0x3ac, 0x3ae, 0x3b1, 0x3b7, 0x3b9, 0x3c1, 0x3c5, 0x3c9, 0x3ce, 0x565,
	0x574, 0x57e, 0x1f00, 0x1f01, 0x1f02, 0x1f03, 0x1f04, 0x1f05, 0x1f06, 0x1f07,
	0x1f20, 0x1f21, 0x1f22, 0x1f23, 0x1f24, 0x1f25, 0x1f26, 0x1f27, 0x1f60, 0x1f61,
	0x1f62, 0x1f63, 0x1f64, 0x1f65, 0x1f66, 0x1f67, 0x1f70, 0x1f74, 0x1f7c, 0x110000}

var reCaseFixStringOffsets = [...]int16{
	0x0, 0x1, 0x6, 0x7, 0x8, 0x9, 0xd, 0xe, 0xf, 0x10, 0x11, 0x12, 0x13,
	0x17, 0x1b, 0x20, 0x21, 0x2a, 0x2e, 0x2f, 0x30, 0x34, 0x35, 0x37, 0x39, 0x3b,
	0x3d, 0x3f, 0x41, 0x43, 0x45, 0x47, 0x49, 0x4b, 0x4d, 0x4f, 0x51, 0x53, 0x55,
	0x57, 0x59, 0x5b, 0x5d, 0x5f, 0x61, 0x63, 0x65, 0x66, 0x67, 0}

var reCaseFixCounts = [...]int16{
	0x1, 0x5, 0x1, 0x1, 0x1, 0x4, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x4, 0x4, 0x5, 0x1, 0x9,
	0x4, 0x1, 0x1, 0x4, 0x1, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2,
	0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x2, 0x1, 0x1, 0x1, 0}

var reCaseFixData = [...]uint16{
	0x1e9a, 0xfb00, 0xfb01, 0xfb02, 0xfb03, 0xfb04, 0x1e96, 0x130, 0x1f0, 0xdf, 0x1e9e, 0xfb05,
	0xfb06, 0x1e97, 0x1e98, 0x1e99, 0x149, 0x1fb4, 0x1fc4, 0x1fb3, 0x1fb6, 0x1fb7, 0x1fbc, 0x1fc3,
	0x1fc6, 0x1fc7, 0x1fcc, 0x390, 0x1fd2, 0x1fd3, 0x1fd6, 0x1fd7, 0x1fe4, 0x3b0, 0x1f50, 0x1f52,
	0x1f54, 0x1f56, 0x1fe2, 0x1fe3, 0x1fe6, 0x1fe7, 0x1ff3, 0x1ff6, 0x1ff7, 0x1ffc, 0x1ff4, 0x587,
	0xfb13, 0xfb14, 0xfb15, 0xfb17, 0xfb16, 0x1f80, 0x1f88, 0x1f81, 0x1f89, 0x1f82, 0x1f8a, 0x1f83,
	0x1f8b, 0x1f84, 0x1f8c, 0x1f85, 0x1f8d, 0x1f86, 0x1f8e, 0x1f87, 0x1f8f, 0x1f90, 0x1f98, 0x1f91,
	0x1f99, 0x1f92, 0x1f9a, 0x1f93, 0x1f9b, 0x1f94, 0x1f9c, 0x1f95, 0x1f9d, 0x1f96, 0x1f9e, 0x1f97,
	0x1f9f, 0x1fa0, 0x1fa8, 0x1fa1, 0x1fa9, 0x1fa2, 0x1faa, 0x1fa3, 0x1fab, 0x1fa4, 0x1fac, 0x1fa5,
	0x1fad, 0x1fa6, 0x1fae, 0x1fa7, 0x1faf, 0x1fb2, 0x1fc2, 0x1ff2, 0}

func (c *compiler) findCaseInsensitiveStarters(ch rune, starterChars *uset.UnicodeSet) {
	if uprops.HasBinaryProperty(ch, uprops.UCharCaseSensitive) {
		caseFoldedC := ucase.Fold(ch)
		starterChars.Clear()
		starterChars.AddRune(caseFoldedC)

		var i int
		for i = 0; reCaseFixCodePoints[i] < ch; i++ {
			// Simple linear search through the sorted list of interesting code points.
		}

		if reCaseFixCodePoints[i] == ch {
			data := reCaseFixData[reCaseFixStringOffsets[i]:]
			numCharsToAdd := reCaseFixCounts[i]
			for j := int16(0); j < numCharsToAdd; j++ {
				var cpToAdd rune
				cpToAdd, data = utf16.NextUnsafe(data)
				starterChars.AddRune(cpToAdd)
			}
		}

		starterChars.CloseOver(uset.CaseInsensitive)
	} else {
		// Not a cased character. Just return it alone.
		starterChars.Clear()
		starterChars.AddRune(ch)
	}
}

func (c *compiler) scanProp() *uset.UnicodeSet {
	if c.err != nil {
		return nil
	}
	negated := c.c.char == chP

	c.nextChar(&c.c)
	if c.c.char != chLBrace {
		c.error(PropertySyntax)
		return nil
	}

	var propertyName strings.Builder
	for {
		c.nextChar(&c.c)
		if c.c.char == chRBrace {
			break
		}
		if c.c.char == -1 {
			c.error(PropertySyntax)
			return nil
		}
		propertyName.WriteRune(c.c.char)
	}

	ss := c.createSetForProperty(propertyName.String(), negated)
	c.nextChar(&c.c)
	return ss
}

func (c *compiler) createSetForProperty(propName string, negated bool) *uset.UnicodeSet {
	if c.err != nil {
		return nil
	}

	var set *uset.UnicodeSet

	var usetFlags uset.USet
	if c.modeFlags&CaseInsensitive != 0 {
		usetFlags |= uset.CaseInsensitive
	}

	var err error
	set, err = uprops.NewUnicodeSetFomPattern("\\p{"+propName+"}", usetFlags)
	if err == nil {
		goto done
	}

	//
	//  The incoming property wasn't directly recognized by ICU.

	//  Check [:word:] and [:all:]. These are not recognized as a properties by ICU UnicodeSet.
	//     Java accepts 'word' with mixed case.
	//     Java accepts 'all' only in all lower case.
	if strings.EqualFold(propName, "word") {
		set = staticPropertySets[urxIswordSet].Clone()
		goto done
	}
	if propName == "all" {
		set = uset.New()
		set.AddRuneRange(0, 0x10ffff)
		goto done
	}

	//    Do Java InBlock expressions
	//
	if strings.HasPrefix(propName, "In") && len(propName) >= 3 {
		set = uset.New()
		if uprops.ApplyPropertyAlias(set, "Block", propName[2:]) != nil {
			c.error(PropertySyntax)
		}
		goto done
	}

	//  Check for the Java form "IsBooleanPropertyValue", which we will recast
	//  as "BooleanPropertyValue". The property value can be either a
	//  a General Category or a Script Name.
	if strings.HasPrefix(propName, "Is") && len(propName) >= 3 {
		mPropName := propName[2:]
		if strings.IndexByte(mPropName, '=') >= 0 {
			c.error(PropertySyntax)
			goto done
		}

		if strings.EqualFold(mPropName, "assigned") {
			mPropName = "unassigned"
			negated = !negated
		} else if strings.EqualFold(mPropName, "TitleCase") {
			mPropName = "Titlecase_Letter"
		}

		set, err = uprops.NewUnicodeSetFomPattern("\\p{"+mPropName+"}", 0)
		if err != nil {
			c.error(PropertySyntax)
		} else if !set.IsEmpty() && (usetFlags&uset.CaseInsensitive) != 0 {
			set.CloseOver(uset.CaseInsensitive)
		}
		goto done
	}

	if strings.HasPrefix(propName, "java") {
		set = uset.New()

		//
		//  Try the various Java specific properties.
		//   These all begin with "java"
		//
		if propName == "javaDefined" {
			c.err = uprops.AddCategory(set, uchar.GcCnMask)
			set.Complement()
		} else if propName == "javaDigit" {
			c.err = uprops.AddCategory(set, uchar.GcNdMask)
		} else if propName == "javaIdentifierIgnorable" {
			c.err = addIdentifierIgnorable(set)
		} else if propName == "javaISOControl" {
			set.AddRuneRange(0, 0x1F)
			set.AddRuneRange(0x7F, 0x9F)
		} else if propName == "javaJavaIdentifierPart" {
			c.err = uprops.AddCategory(set, uchar.GcLMask)
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcScMask)
			}
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcPcMask)
			}
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcNdMask)
			}
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcNlMask)
			}
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcMcMask)
			}
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcMnMask)
			}
			if c.err == nil {
				c.err = addIdentifierIgnorable(set)
			}
		} else if propName == "javaJavaIdentifierStart" {
			c.err = uprops.AddCategory(set, uchar.GcLMask)
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcNlMask)
			}
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcScMask)
			}
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcPcMask)
			}
		} else if propName == "javaLetter" {
			c.err = uprops.AddCategory(set, uchar.GcLMask)
		} else if propName == "javaLetterOrDigit" {
			c.err = uprops.AddCategory(set, uchar.GcLMask)
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcNdMask)
			}
		} else if propName == "javaLowerCase" {
			c.err = uprops.AddCategory(set, uchar.GcLlMask)
		} else if propName == "javaMirrored" {
			c.err = uprops.ApplyIntPropertyValue(set, uprops.UCharBidiMirrored, 1)
		} else if propName == "javaSpaceChar" {
			c.err = uprops.AddCategory(set, uchar.GcZMask)
		} else if propName == "javaSupplementaryCodePoint" {
			set.AddRuneRange(0x10000, uset.MaxValue)
		} else if propName == "javaTitleCase" {
			c.err = uprops.AddCategory(set, uchar.GcLtMask)
		} else if propName == "javaUnicodeIdentifierStart" {
			c.err = uprops.AddCategory(set, uchar.GcLMask)
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcNlMask)
			}
		} else if propName == "javaUnicodeIdentifierPart" {
			c.err = uprops.AddCategory(set, uchar.GcLMask)
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcPcMask)
			}
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcNdMask)
			}
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcNlMask)
			}
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcMcMask)
			}
			if c.err == nil {
				c.err = uprops.AddCategory(set, uchar.GcMnMask)
			}
			if c.err == nil {
				c.err = addIdentifierIgnorable(set)
			}
		} else if propName == "javaUpperCase" {
			c.err = uprops.AddCategory(set, uchar.GcLuMask)
		} else if propName == "javaValidCodePoint" {
			set.AddRuneRange(0, uset.MaxValue)
		} else if propName == "javaWhitespace" {
			c.err = uprops.AddCategory(set, uchar.GcZMask)
			excl := uset.New()
			excl.AddRune(0x0a)
			excl.AddRune(0x2007)
			excl.AddRune(0x202f)
			set.RemoveAll(excl)
			set.AddRuneRange(9, 0x0d)
			set.AddRuneRange(0x1c, 0x1f)
		} else {
			c.error(PropertySyntax)
		}

		if c.err == nil && !set.IsEmpty() && (usetFlags&uset.CaseInsensitive) != 0 {
			set.CloseOver(uset.CaseInsensitive)
		}
		goto done
	}

	// Unrecognized property. ICU didn't like it as it was, and none of the Java compatibility
	// extensions matched it.
	c.error(PropertySyntax)

done:
	if c.err != nil {
		return nil
	}
	if negated {
		set.Complement()
	}
	return set
}

func addIdentifierIgnorable(set *uset.UnicodeSet) error {
	set.AddRuneRange(0, 8)
	set.AddRuneRange(0x0e, 0x1b)
	set.AddRuneRange(0x7f, 0x9f)

	return uprops.AddCategory(set, uchar.GcCfMask)
}

func (c *compiler) scanPosixProp() *uset.UnicodeSet {
	var set *uset.UnicodeSet

	if !(c.c.char == chColon) {
		panic("assertion failed: c.lastChar == ':'")
	}

	savedScanIndex := c.scanIndex
	savedScanPattern := c.p
	savedQuoteMode := c.quoteMode
	savedInBackslashQuote := c.inBackslashQuote
	savedEOLComments := c.eolComments
	savedLineNum := c.lineNum
	savedCharNum := c.charNum
	savedLastChar := c.lastChar
	savedPeekChar := c.peekChar
	savedC := c.c

	// Scan for a closing ].   A little tricky because there are some perverse
	//   edge cases possible.  "[:abc\Qdef:] \E]"  is a valid non-property expression,
	//   ending on the second closing ].
	var propName []rune
	negated := false

	// Check for and consume the '^' in a negated POSIX property, e.g.  [:^Letter:]
	c.nextChar(&c.c)
	if c.c.char == chUp {
		negated = true
		c.nextChar(&c.c)
	}

	// Scan for the closing ":]", collecting the property name along the way.
	sawPropSetTerminator := false
	for {
		propName = append(propName, c.c.char)
		c.nextChar(&c.c)
		if c.c.quoted || c.c.char == -1 {
			// Escaped characters or end of input - either says this isn't a [:Property:]
			break
		}
		if c.c.char == chColon {
			c.nextChar(&c.c)
			if c.c.char == chRBracket {
				sawPropSetTerminator = true
				break
			}
		}
	}

	if sawPropSetTerminator {
		set = c.createSetForProperty(string(propName), negated)
	} else {
		// No closing ']' - not a [:Property:]
		//  Restore the original scan position.
		//  The main scanner will retry the input as a normal set expression,
		//    not a [:Property:] expression.
		c.scanIndex = savedScanIndex
		c.p = savedScanPattern
		c.quoteMode = savedQuoteMode
		c.inBackslashQuote = savedInBackslashQuote
		c.eolComments = savedEOLComments
		c.lineNum = savedLineNum
		c.charNum = savedCharNum
		c.lastChar = savedLastChar
		c.peekChar = savedPeekChar
		c.c = savedC
	}

	return set
}

func (c *compiler) compileSet(set *uset.UnicodeSet) {
	if set == nil {
		return
	}
	//  Remove any strings from the set.
	//  There shoudn't be any, but just in case.
	//     (Case Closure can add them; if we had a simple case closure available that
	//      ignored strings, that would be better.)
	setSize := set.Len()

	switch setSize {
	case 0:
		// Set of no elements.   Always fails to match.
		c.appendOp(urxBacktrack, 0)

	case 1:
		// The set contains only a single code point.  Put it into
		//   the compiled pattern as a single char operation rather
		//   than a set, and discard the set itself.
		c.literalChar(set.RuneAt(0))

	default:
		//  The set contains two or more chars.  (the normal case)
		//  Put it into the compiled pattern as a set.
		// theSet->freeze();
		setNumber := len(c.out.sets)
		c.out.sets = append(c.out.sets, set)
		c.appendOp(urxSetref, setNumber)
	}
}

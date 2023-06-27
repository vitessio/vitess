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
)

func (pat *Pattern) Dump(w io.Writer) {
	fmt.Fprintf(w, "Original Pattern:  \"%s\"\n", pat.pattern)
	fmt.Fprintf(w, "   Min Match Length:  %d\n", pat.minMatchLen)
	fmt.Fprintf(w, "   Match Start Type:  %v\n", pat.startType)
	if pat.startType == START_STRING {
		fmt.Fprintf(w, "   Initial match string: \"%s\"\n", string(pat.literalText[pat.initialStringIdx:pat.initialStringIdx+pat.initialStringLen]))
	} else if pat.startType == START_SET {
		fmt.Fprintf(w, "    Match First Chars: %s\n", pat.initialChars.String())
	} else if pat.startType == START_CHAR {
		fmt.Fprintf(w, "    First char of Match: ")
		if pat.initialChar > 0x20 {
			fmt.Fprintf(w, "'%c'\n", pat.initialChar)
		} else {
			fmt.Fprintf(w, "%#x\n", pat.initialChar)
		}
	}

	fmt.Fprintf(w, "Named Capture Groups:\n")
	if len(pat.namedCaptureMap) == 0 {
		fmt.Fprintf(w, "   None\n")
	} else {
		for name, number := range pat.namedCaptureMap {
			fmt.Fprintf(w, "   %d\t%s\n", number, name)
		}
	}

	fmt.Fprintf(w, "\nIndex   Binary     Type             Operand\n-------------------------------------------\n")
	for idx := range pat.compiledPat {
		pat.dumpOp(w, idx)
	}
	fmt.Fprintf(w, "\n\n")
}

func (pat *Pattern) dumpOp(w io.Writer, index int) {
	op := pat.compiledPat[index]
	val := op.Value()
	opType := op.Type()
	pinnedType := opType
	if int(pinnedType) >= len(UrxOpcodeNames) {
		pinnedType = 0
	}

	fmt.Fprintf(w, "%4d   %08x    %-15s  ", index, op, UrxOpcodeNames[pinnedType])

	switch opType {
	case URX_NOP,
		URX_DOTANY,
		URX_DOTANY_ALL,
		URX_FAIL,
		URX_CARET,
		URX_DOLLAR,
		URX_BACKSLASH_G,
		URX_BACKSLASH_X,
		URX_END,
		URX_DOLLAR_M,
		URX_CARET_M:
		// Types with no operand field of interest.

	case URX_RESERVED_OP,
		URX_START_CAPTURE,
		URX_END_CAPTURE,
		URX_STATE_SAVE,
		URX_JMP,
		URX_JMP_SAV,
		URX_JMP_SAV_X,
		URX_BACKSLASH_B,
		URX_BACKSLASH_BU,
		URX_BACKSLASH_D,
		URX_BACKSLASH_Z,
		URX_STRING_LEN,
		URX_CTR_INIT,
		URX_CTR_INIT_NG,
		URX_CTR_LOOP,
		URX_CTR_LOOP_NG,
		URX_RELOC_OPRND,
		URX_STO_SP,
		URX_LD_SP,
		URX_BACKREF,
		URX_STO_INP_LOC,
		URX_JMPX,
		URX_LA_START,
		URX_LA_END,
		URX_BACKREF_I,
		URX_LB_START,
		URX_LB_CONT,
		URX_LB_END,
		URX_LBN_CONT,
		URX_LBN_END,
		URX_LOOP_C,
		URX_LOOP_DOT_I,
		URX_BACKSLASH_H,
		URX_BACKSLASH_R,
		URX_BACKSLASH_V:
		// types with an integer operand field.
		fmt.Fprintf(w, "%d", val)

	case URX_ONECHAR, URX_ONECHAR_I:
		if val < 0x20 {
			fmt.Fprintf(w, "%#x", val)
		} else {
			fmt.Fprintf(w, "'%c'", rune(val))
		}

	case URX_STRING, URX_STRING_I:
		lengthOp := pat.compiledPat[index+1]
		// U_ASSERT(URX_TYPE(lengthOp) == URX_STRING_LEN);
		length := lengthOp.Value()
		fmt.Fprintf(w, "%q", string(pat.literalText[val:val+length]))

	case URX_SETREF, URX_LOOP_SR_I:
		// UnicodeString s;
		// UnicodeSet *set = (UnicodeSet *)fSets->elementAt(val);
		//set->toPattern(s, TRUE);
		fmt.Fprintf(w, "%s", pat.sets[val].String())

	case URX_STATIC_SETREF, URX_STAT_SETREF_N:
		if (val & URX_NEG_SET) != 0 {
			fmt.Fprintf(w, "NOT ")
			val &= ^URX_NEG_SET
		}
		// UnicodeSet &set = RegexStaticSets::gStaticSets->fPropSets[val];
		// set.toPattern(s, TRUE);
		fmt.Fprintf(w, "%s", staticPropertySets[val].String())

	default:
		fmt.Fprintf(w, "??????")
	}
	fmt.Fprintf(w, "\n")
}

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
	if pat.startType == startString {
		fmt.Fprintf(w, "   Initial match string: \"%s\"\n", string(pat.literalText[pat.initialStringIdx:pat.initialStringIdx+pat.initialStringLen]))
	} else if pat.startType == startSet {
		fmt.Fprintf(w, "    Match First Chars: %s\n", pat.initialChars.String())
	} else if pat.startType == startChar {
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
	val := op.value()
	opType := op.typ()
	pinnedType := opType
	if int(pinnedType) >= len(urxOpcodeNames) {
		pinnedType = 0
	}

	fmt.Fprintf(w, "%4d   %08x    %-15s  ", index, op, urxOpcodeNames[pinnedType])

	switch opType {
	case urxNop,
		urxDotany,
		urxDotanyAll,
		urxFail,
		urxCaret,
		urxDollar,
		urxBackslashG,
		urxBackslashX,
		urxEnd,
		urxDollarM,
		urxCaretM:
		// Types with no operand field of interest.

	case urxReservedOp,
		urxStartCapture,
		urxEndCapture,
		urxStateSave,
		urxJmp,
		urxJmpSav,
		urxJmpSavX,
		urxBackslashB,
		urxBackslashBu,
		urxBackslashD,
		urxBackslashZ,
		urxStringLen,
		urxCtrInit,
		urxCtrInitNg,
		utxCtrLoop,
		urxCtrLoopNg,
		urxRelocOprnd,
		urxStoSp,
		urxLdSp,
		urxBackref,
		urxStoInpLoc,
		urxJmpx,
		urxLaStart,
		urxLaEnd,
		urxBackrefI,
		urxLbStart,
		urxLbCont,
		urxLbEnd,
		urxLbnCount,
		urxLbnEnd,
		urxLoopC,
		urxLoopDotI,
		urxBackslashH,
		urxBackslashR,
		urxBackslashV:
		// types with an integer operand field.
		fmt.Fprintf(w, "%d", val)

	case urxOnechar, urcOnecharI:
		if val < 0x20 {
			fmt.Fprintf(w, "%#x", val)
		} else {
			fmt.Fprintf(w, "'%c'", rune(val))
		}

	case urxString, urxStringI:
		lengthOp := pat.compiledPat[index+1]
		length := lengthOp.value()
		fmt.Fprintf(w, "%q", string(pat.literalText[val:val+length]))

	case urxSetref, urxLoopSrI:
		fmt.Fprintf(w, "%s", pat.sets[val].String())

	case urxStaticSetref, urxStatSetrefN:
		if (val & urxNegSet) != 0 {
			fmt.Fprintf(w, "NOT ")
			val &= ^urxNegSet
		}
		fmt.Fprintf(w, "%s", staticPropertySets[val].String())

	default:
		fmt.Fprintf(w, "??????")
	}
	fmt.Fprintf(w, "\n")
}

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
	"strings"

	"vitess.io/vitess/go/mysql/icuregex/internal/uerror"
)

type CompileError struct {
	Code    uerror.URegexCompileErrorCode
	Line    int
	Offset  int
	Context string
}

func (e *CompileError) Error() string {
	var out strings.Builder
	switch e.Code {
	case uerror.U_REGEX_INTERNAL_ERROR:
		out.WriteString("Internal Error")
	case uerror.U_REGEX_RULE_SYNTAX:
		out.WriteString("Syntax Error")
	case uerror.U_REGEX_INVALID_STATE:
		out.WriteString("Invalid State")
	case uerror.U_REGEX_BAD_ESCAPE_SEQUENCE:
		out.WriteString("Bad escape sequence")
	case uerror.U_REGEX_PROPERTY_SYNTAX:
		out.WriteString("Property syntax error")
	case uerror.U_REGEX_UNIMPLEMENTED:
		out.WriteString("Unimplemented")
	case uerror.U_REGEX_MISMATCHED_PAREN:
		out.WriteString("Mismatched parentheses")
	case uerror.U_REGEX_NUMBER_TOO_BIG:
		out.WriteString("Number too big")
	case uerror.U_REGEX_BAD_INTERVAL:
		out.WriteString("Bad interval")
	case uerror.U_REGEX_MAX_LT_MIN:
		out.WriteString("Max less than min")
	case uerror.U_REGEX_INVALID_BACK_REF:
		out.WriteString("Invalid back reference")
	case uerror.U_REGEX_INVALID_FLAG:
		out.WriteString("Invalid flag")
	case uerror.U_REGEX_LOOK_BEHIND_LIMIT:
		out.WriteString("Look behind limit")
	case uerror.U_REGEX_SET_CONTAINS_STRING:
		out.WriteString("Set contains string")
	case uerror.U_REGEX_MISSING_CLOSE_BRACKET:
		out.WriteString("Missing closing ]")
	case uerror.U_REGEX_INVALID_RANGE:
		out.WriteString("Invalid range")
	case uerror.U_REGEX_PATTERN_TOO_BIG:
		out.WriteString("Pattern too big")
	case uerror.U_REGEX_INVALID_CAPTURE_GROUP_NAME:
		out.WriteString("Invalid capture group name")
	}
	_, _ = fmt.Fprintf(&out, " at line %d, column %d: `%s`", e.Line, e.Offset, e.Context)

	return out.String()
}

type MatchError struct {
	Code     uerror.URegexMatchErrorCode
	Pattern  string
	Position int
	Input    []rune
}

const maxMatchInputLength = 20

func (e *MatchError) Error() string {
	var out strings.Builder
	switch e.Code {
	case uerror.U_REGEX_STACK_OVERFLOW:
		out.WriteString("Stack overflow")
	case uerror.U_REGEX_TIME_OUT:
		out.WriteString("Timeout")
	}

	input := e.Input
	if len(input) > maxMatchInputLength {
		var b []rune
		start := e.Position - maxMatchInputLength/2
		if start < 0 {
			start = 0
		} else {
			b = append(b, '.', '.', '.')
		}
		end := start + maxMatchInputLength
		trailing := true
		if end > len(input) {
			end = len(input)
			trailing = false
		}
		b = append(b, input[start:end]...)
		if trailing {
			b = append(b, '.', '.', '.')
		}
		input = b
	}
	_, _ = fmt.Fprintf(&out, " for expression `%s` at position %d in: %q", e.Pattern, e.Position, string(input))

	return out.String()
}

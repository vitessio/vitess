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
)

type CompileError struct {
	Code    CompileErrorCode
	Line    int
	Offset  int
	Context string
}

func (e *CompileError) Error() string {
	var out strings.Builder
	switch e.Code {
	case InternalError:
		out.WriteString("Internal error")
	case RuleSyntax:
		out.WriteString("Syntax error")
	case BadEscapeSequence:
		out.WriteString("Bad escape sequence")
	case PropertySyntax:
		out.WriteString("Property syntax error")
	case Unimplemented:
		out.WriteString("Unimplemented")
	case MismatchedParen:
		out.WriteString("Mismatched parentheses")
	case NumberTooBig:
		out.WriteString("Number too big")
	case BadInterval:
		out.WriteString("Bad interval")
	case MaxLtMin:
		out.WriteString("Max less than min")
	case InvalidBackRef:
		out.WriteString("Invalid back reference")
	case InvalidFlag:
		out.WriteString("Invalid flag")
	case LookBehindLimit:
		out.WriteString("Look behind limit")
	case MissingCloseBracket:
		out.WriteString("Missing closing ]")
	case InvalidRange:
		out.WriteString("Invalid range")
	case PatternTooBig:
		out.WriteString("Pattern too big")
	case InvalidCaptureGroupName:
		out.WriteString("Invalid capture group name")
	}
	_, _ = fmt.Fprintf(&out, " in regular expression on line %d, character %d: `%s`", e.Line, e.Offset, e.Context)

	return out.String()
}

type MatchError struct {
	Code     MatchErrorCode
	Pattern  string
	Position int
	Input    []rune
}

const maxMatchInputLength = 20

func (e *MatchError) Error() string {
	var out strings.Builder
	switch e.Code {
	case StackOverflow:
		out.WriteString("Stack overflow")
	case TimeOut:
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

type Code int32

type CompileErrorCode int32

const (
	InternalError           CompileErrorCode = iota + 1 /**< An internal error (bug) was detected.              */
	RuleSyntax                                          /**< Syntax error in regexp pattern.                    */
	BadEscapeSequence                                   /**< Unrecognized backslash escape sequence in pattern  */
	PropertySyntax                                      /**< Incorrect Unicode property                         */
	Unimplemented                                       /**< Use of regexp feature that is not yet implemented. */
	MismatchedParen                                     /**< Incorrectly nested parentheses in regexp pattern.  */
	NumberTooBig                                        /**< Decimal number is too large.                       */
	BadInterval                                         /**< Error in {min,max} interval                        */
	MaxLtMin                                            /**< In {min,max}, max is less than min.                */
	InvalidBackRef                                      /**< Back-reference to a non-existent capture group.    */
	InvalidFlag                                         /**< Invalid value for match mode flags.                */
	LookBehindLimit                                     /**< Look-Behind pattern matches must have a bounded maximum length.    */
	MissingCloseBracket                                 /**< Missing closing bracket on a bracket expression. */
	InvalidRange                                        /**< In a character range [x-y], x is greater than y.   */
	PatternTooBig                                       /**< Pattern exceeds limits on size or complexity. @stable ICU 55 */
	InvalidCaptureGroupName                             /**< Invalid capture group name. @stable ICU 55 */
)

type MatchErrorCode int32

const (
	StackOverflow MatchErrorCode = iota /**< Regular expression backtrack stack overflow.       */
	TimeOut                             /**< Maximum allowed match time exceeded                */
)

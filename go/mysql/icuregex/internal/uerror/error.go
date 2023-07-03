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

package uerror

import (
	"errors"
)

type Code int32

var ErrIllegalArgument = errors.New("illegal argument")
var ErrUnsupported = errors.New("unsupported")

type CompileErrorCode int32

const (
	ZeroError               CompileErrorCode = iota
	InternalError                            /**< An internal error (bug) was detected.              */
	RuleSyntax                               /**< Syntax error in regexp pattern.                    */
	InvalidState                             /**< RegexMatcher in invalid state for requested operation */
	BadEscapeSequence                        /**< Unrecognized backslash escape sequence in pattern  */
	PropertySyntax                           /**< Incorrect Unicode property                         */
	Unimplemented                            /**< Use of regexp feature that is not yet implemented. */
	MismatchedParen                          /**< Incorrectly nested parentheses in regexp pattern.  */
	NumberTooBig                             /**< Decimal number is too large.                       */
	BadInterval                              /**< Error in {min,max} interval                        */
	MaxLtMin                                 /**< In {min,max}, max is less than min.                */
	InvalidBackRef                           /**< Back-reference to a non-existent capture group.    */
	InvalidFlag                              /**< Invalid value for match mode flags.                */
	LookBehindLimit                          /**< Look-Behind pattern matches must have a bounded maximum length.    */
	SetContainsString                        /**< Regexps cannot have UnicodeSets containing strings.*/
	MissingCloseBracket                      /**< Missing closing bracket on a bracket expression. */
	InvalidRange                             /**< In a character range [x-y], x is greater than y.   */
	PatternTooBig                            /**< Pattern exceeds limits on size or complexity. @stable ICU 55 */
	InvalidCaptureGroupName                  /**< Invalid capture group name. @stable ICU 55 */
)

type MatchErrorCode int32

const (
	StackOverflow MatchErrorCode = iota /**< Regular expression backtrack stack overflow.       */
	TimeOut                             /**< Maximum allowed match time exceeded                */
)

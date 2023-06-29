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

type UErrorCode int32

var IllegalArgumentError = errors.New("illegal argument")
var UnsupportedError = errors.New("unsupported")

type URegexCompileErrorCode int32

const (
	U_REGEX_ZERO_ERROR                 URegexCompileErrorCode = iota
	U_REGEX_INTERNAL_ERROR                                    /**< An internal error (bug) was detected.              */
	U_REGEX_RULE_SYNTAX                                       /**< Syntax error in regexp pattern.                    */
	U_REGEX_INVALID_STATE                                     /**< RegexMatcher in invalid state for requested operation */
	U_REGEX_BAD_ESCAPE_SEQUENCE                               /**< Unrecognized backslash escape sequence in pattern  */
	U_REGEX_PROPERTY_SYNTAX                                   /**< Incorrect Unicode property                         */
	U_REGEX_UNIMPLEMENTED                                     /**< Use of regexp feature that is not yet implemented. */
	U_REGEX_MISMATCHED_PAREN                                  /**< Incorrectly nested parentheses in regexp pattern.  */
	U_REGEX_NUMBER_TOO_BIG                                    /**< Decimal number is too large.                       */
	U_REGEX_BAD_INTERVAL                                      /**< Error in {min,max} interval                        */
	U_REGEX_MAX_LT_MIN                                        /**< In {min,max}, max is less than min.                */
	U_REGEX_INVALID_BACK_REF                                  /**< Back-reference to a non-existent capture group.    */
	U_REGEX_INVALID_FLAG                                      /**< Invalid value for match mode flags.                */
	U_REGEX_LOOK_BEHIND_LIMIT                                 /**< Look-Behind pattern matches must have a bounded maximum length.    */
	U_REGEX_SET_CONTAINS_STRING                               /**< Regexps cannot have UnicodeSets containing strings.*/
	U_REGEX_MISSING_CLOSE_BRACKET                             /**< Missing closing bracket on a bracket expression. */
	U_REGEX_INVALID_RANGE                                     /**< In a character range [x-y], x is greater than y.   */
	U_REGEX_PATTERN_TOO_BIG                                   /**< Pattern exceeds limits on size or complexity. @stable ICU 55 */
	U_REGEX_INVALID_CAPTURE_GROUP_NAME                        /**< Invalid capture group name. @stable ICU 55 */
	U_REGEX_UNSUPPORTED_ERROR                                 /**< Use of an unsupported feature. @stable ICU 55 */
)

type URegexMatchErrorCode int32

const (
	U_REGEX_STACK_OVERFLOW URegexMatchErrorCode = iota /**< Regular expression backtrack stack overflow.       */
	U_REGEX_TIME_OUT                                   /**< Maximum allowed match time exceeded                */
)

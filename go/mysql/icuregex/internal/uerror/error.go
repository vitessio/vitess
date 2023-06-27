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

import "fmt"

type UErrorCode int32

const (
	U_ZERO_ERROR                  UErrorCode = iota /**< No error, no warning. */
	U_ILLEGAL_ARGUMENT_ERROR                        /**< Start of codes indicating failure */
	U_MISSING_RESOURCE_ERROR                        /**< The requested resource cannot be found */
	U_INVALID_FORMAT_ERROR                          /**< Data format is not what is expected */
	U_FILE_ACCESS_ERROR                             /**< The requested file cannot be found */
	U_INTERNAL_PROGRAM_ERROR                        /**< Indicates a bug in the library code */
	U_MESSAGE_PARSE_ERROR                           /**< Unable to parse a message (message format) */
	U_MEMORY_ALLOCATION_ERROR                       /**< Memory allocation error */
	U_INDEX_OUTOFBOUNDS_ERROR                       /**< Trying to access the index that is out of bounds */
	U_PARSE_ERROR                                   /**< Equivalent to Java ParseException */
	U_INVALID_CHAR_FOUND                            /**< Character conversion: Unmappable input sequence. In other APIs: Invalid character. */
	U_TRUNCATED_CHAR_FOUND                          /**< Character conversion: Incomplete input sequence. */
	U_ILLEGAL_CHAR_FOUND                            /**< Character conversion: Illegal input sequence/combination of input units. */
	U_INVALID_TABLE_FORMAT                          /**< Conversion table file found, but corrupted */
	U_INVALID_TABLE_FILE                            /**< Conversion table file not found */
	U_BUFFER_OVERFLOW_ERROR                         /**< A result would not fit in the supplied buffer */
	U_UNSUPPORTED_ERROR                             /**< Requested operation not supported in current context */
	U_RESOURCE_TYPE_MISMATCH                        /**< an operation is requested over a resource that does not support it */
	U_ILLEGAL_ESCAPE_SEQUENCE                       /**< ISO-2022 illegal escape sequence */
	U_UNSUPPORTED_ESCAPE_SEQUENCE                   /**< ISO-2022 unsupported escape sequence */
	U_NO_SPACE_AVAILABLE                            /**< No space available for in-buffer expansion for Arabic shaping */
	U_CE_NOT_FOUND_ERROR                            /**< Currently used only while setting variable top, but can be used generally */
	U_PRIMARY_TOO_LONG_ERROR                        /**< User tried to set variable top to a primary that is longer than two bytes */
	U_STATE_TOO_OLD_ERROR                           /**< ICU cannot construct a service from this state, as it is no longer supported */
	U_TOO_MANY_ALIASES_ERROR                        /**< There are too many aliases in the path to the requested resource.
	  It is very possible that a circular alias definition has occurred */
	U_ENUM_OUT_OF_SYNC_ERROR     /**< UEnumeration out of sync with underlying collection */
	U_INVARIANT_CONVERSION_ERROR /**< Unable to convert a UChar* string to char* with the invariant converter. */
	U_INVALID_STATE_ERROR        /**< Requested operation can not be completed with ICU in its current state */
	U_COLLATOR_VERSION_MISMATCH  /**< Collator version is not compatible with the base version */
	U_USELESS_COLLATOR_ERROR     /**< Collator is options only and no base is specified */
	U_NO_WRITE_PERMISSION        /**< Attempt to modify read-only or constant data. */
	U_INPUT_TOO_LONG_ERROR
)

/*
 * Error codes in the range 0x10000 0x10100 are reserved for Transliterator.
 */
const (
	U_BAD_VARIABLE_DEFINITION       UErrorCode = iota + 0x10000 /**< Missing '$' or duplicate variable name */
	U_MALFORMED_RULE                                            /**< Elements of a rule are misplaced */
	U_MALFORMED_SET                                             /**< A UnicodeSet pattern is invalid*/
	U_MALFORMED_SYMBOL_REFERENCE                                /**< UNUSED as of ICU 2.4 */
	U_MALFORMED_UNICODE_ESCAPE                                  /**< A Unicode escape pattern is invalid*/
	U_MALFORMED_VARIABLE_DEFINITION                             /**< A variable definition is invalid */
	U_MALFORMED_VARIABLE_REFERENCE                              /**< A variable reference is invalid */
	U_MISMATCHED_SEGMENT_DELIMITERS                             /**< UNUSED as of ICU 2.4 */
	U_MISPLACED_ANCHOR_START                                    /**< A start anchor appears at an illegal position */
	U_MISPLACED_CURSOR_OFFSET                                   /**< A cursor offset occurs at an illegal position */
	U_MISPLACED_QUANTIFIER                                      /**< A quantifier appears after a segment close delimiter */
	U_MISSING_OPERATOR                                          /**< A rule contains no operator */
	U_MISSING_SEGMENT_CLOSE                                     /**< UNUSED as of ICU 2.4 */
	U_MULTIPLE_ANTE_CONTEXTS                                    /**< More than one ante context */
	U_MULTIPLE_CURSORS                                          /**< More than one cursor */
	U_MULTIPLE_POST_CONTEXTS                                    /**< More than one post context */
	U_TRAILING_BACKSLASH                                        /**< A dangling backslash */
	U_UNDEFINED_SEGMENT_REFERENCE                               /**< A segment reference does not correspond to a defined segment */
	U_UNDEFINED_VARIABLE                                        /**< A variable reference does not correspond to a defined variable */
	U_UNQUOTED_SPECIAL                                          /**< A special character was not quoted or escaped */
	U_UNTERMINATED_QUOTE                                        /**< A closing single quote is missing */
	U_RULE_MASK_ERROR                                           /**< A rule is hidden by an earlier more general rule */
	U_MISPLACED_COMPOUND_FILTER                                 /**< A compound filter is in an invalid location */
	U_MULTIPLE_COMPOUND_FILTERS                                 /**< More than one compound filter */
	U_INVALID_RBT_SYNTAX                                        /**< A "::id" rule was passed to the RuleBasedTransliterator parser */
	U_INVALID_PROPERTY_PATTERN                                  /**< UNUSED as of ICU 2.4 */
	U_MALFORMED_PRAGMA                                          /**< A 'use' pragma is invalid */
	U_UNCLOSED_SEGMENT                                          /**< A closing ')' is missing */
	U_ILLEGAL_CHAR_IN_SEGMENT                                   /**< UNUSED as of ICU 2.4 */
	U_VARIABLE_RANGE_EXHAUSTED                                  /**< Too many stand-ins generated for the given variable range */
	U_VARIABLE_RANGE_OVERLAP                                    /**< The variable range overlaps characters used in rules */
	U_ILLEGAL_CHARACTER                                         /**< A special character is outside its allowed context */
	U_INTERNAL_TRANSLITERATOR_ERROR                             /**< Internal transliterator system error */
	U_INVALID_ID                                                /**< A "::id" rule specifies an unknown transliterator */
	U_INVALID_FUNCTION                                          /**< A "&fn()" rule specifies an unknown transliterator */
)

/*
 * Error codes in the range 0x10200 0x102ff are reserved for BreakIterator.
 */
const (
	U_BRK_INTERNAL_ERROR            UErrorCode = iota + 0x10200 /**< An internal error (bug) was detected.             */
	U_BRK_HEX_DIGITS_EXPECTED                                   /**< Hex digits expected as part of a escaped char in a rule. */
	U_BRK_SEMICOLON_EXPECTED                                    /**< Missing ';' at the end of a RBBI rule.            */
	U_BRK_RULE_SYNTAX                                           /**< Syntax error in RBBI rule.                        */
	U_BRK_UNCLOSED_SET                                          /**< UnicodeSet writing an RBBI rule missing a closing ']'. */
	U_BRK_ASSIGN_ERROR                                          /**< Syntax error in RBBI rule assignment statement.   */
	U_BRK_VARIABLE_REDFINITION                                  /**< RBBI rule $Variable redefined.                    */
	U_BRK_MISMATCHED_PAREN                                      /**< Mis-matched parentheses in an RBBI rule.          */
	U_BRK_NEW_LINE_IN_QUOTED_STRING                             /**< Missing closing quote in an RBBI rule.            */
	U_BRK_UNDEFINED_VARIABLE                                    /**< Use of an undefined $Variable in an RBBI rule.    */
	U_BRK_INIT_ERROR                                            /**< Initialization failure.  Probable missing ICU Data. */
	U_BRK_RULE_EMPTY_SET                                        /**< Rule contains an empty Unicode Set.               */
	U_BRK_UNRECOGNIZED_OPTION                                   /**< !!option in RBBI rules not recognized.            */
	U_BRK_MALFORMED_RULE_TAG                                    /**< The {nnn} tag on a rule is malformed              */
)

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

func (e UErrorCode) Error() string {
	return fmt.Sprintf("UErrorCode: %d", e)
}

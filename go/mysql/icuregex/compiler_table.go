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

type patternParseAction uint8

const (
	doSetBackslashD patternParseAction = iota
	doBackslashh
	doBackslashH
	doSetLiteralEscaped
	doOpenLookAheadNeg
	doCompleteNamedBackRef
	doPatStart
	doBackslashS
	doBackslashD
	doNGStar
	doNOP
	doBackslashX
	doSetLiteral
	doContinueNamedCapture
	doBackslashG
	doBackslashR
	doSetBegin
	doSetBackslashv
	doPossessivePlus
	doPerlInline
	doBackslashZ
	doSetAddAmp
	doSetBeginDifference1
	doIntervalError
	doSetNegate
	doIntervalInit
	doSetIntersection2
	doPossessiveInterval
	doRuleError
	doBackslashW
	doContinueNamedBackRef
	doOpenNonCaptureParen
	doExit
	doSetNamedChar
	doSetBackslashV
	doConditionalExpr
	doEscapeError
	doBadOpenParenType
	doPossessiveStar
	doSetAddDash
	doEscapedLiteralChar
	doSetBackslashw
	doIntervalUpperDigit
	doBackslashv
	doSetBackslashS
	doSetNoCloseError
	doSetProp
	doBackslashB
	doSetEnd
	doSetRange
	doMatchModeParen
	doPlus
	doBackslashV
	doSetMatchMode
	doBackslashz
	doSetNamedRange
	doOpenLookBehindNeg
	doInterval
	doBadNamedCapture
	doBeginMatchMode
	doBackslashd
	doPatFinish
	doNamedChar
	doNGPlus
	doSetDifference2
	doSetBackslashH
	doCloseParen
	doDotAny
	doOpenCaptureParen
	doEnterQuoteMode
	doOpenAtomicParen
	doBadModeFlag
	doSetBackslashd
	doSetFinish
	doProperty
	doBeginNamedBackRef
	doBackRef
	doOpt
	doDollar
	doBeginNamedCapture
	doNGInterval
	doSetOpError
	doSetPosixProp
	doSetBeginIntersection1
	doBackslashb
	doSetBeginUnion
	doIntevalLowerDigit
	doSetBackslashh
	doStar
	doMatchMode
	doBackslashA
	doOpenLookBehind
	doPossessiveOpt
	doOrOperator
	doBackslashw
	doBackslashs
	doLiteralChar
	doSuppressComments
	doCaret
	doIntervalSame
	doNGOpt
	doOpenLookAhead
	doSetBackslashW
	doMismatchedParenErr
	doSetBackslashs
	rbbiLastAction
)

// -------------------------------------------------------------------------------
//
//	RegexTableEl       represents the structure of a row in the transition table
//	                   for the pattern parser state machine.
//
// -------------------------------------------------------------------------------
type regexTableEl struct {
	action    patternParseAction
	charClass uint8
	nextState uint8
	pushState uint8
	nextChar  bool
}

var parseStateTable = []regexTableEl{
	{doNOP, 0, 0, 0, true},
	{doPatStart, 255, 2, 0, false},                        //  1      start
	{doLiteralChar, 254, 14, 0, true},                     //  2      term
	{doLiteralChar, 130, 14, 0, true},                     //  3
	{doSetBegin, 91 /* [ */, 123, 205, true},              //  4
	{doNOP, 40 /* ( */, 27, 0, true},                      //  5
	{doDotAny, 46 /* . */, 14, 0, true},                   //  6
	{doCaret, 94 /* ^ */, 14, 0, true},                    //  7
	{doDollar, 36 /* $ */, 14, 0, true},                   //  8
	{doNOP, 92 /* \ */, 89, 0, true},                      //  9
	{doOrOperator, 124 /* | */, 2, 0, true},               //  10
	{doCloseParen, 41 /* ) */, 255, 0, true},              //  11
	{doPatFinish, 253, 2, 0, false},                       //  12
	{doRuleError, 255, 206, 0, false},                     //  13
	{doNOP, 42 /* * */, 68, 0, true},                      //  14      expr-quant
	{doNOP, 43 /* + */, 71, 0, true},                      //  15
	{doNOP, 63 /* ? */, 74, 0, true},                      //  16
	{doIntervalInit, 123 /* { */, 77, 0, true},            //  17
	{doNOP, 40 /* ( */, 23, 0, true},                      //  18
	{doNOP, 255, 20, 0, false},                            //  19
	{doOrOperator, 124 /* | */, 2, 0, true},               //  20      expr-cont
	{doCloseParen, 41 /* ) */, 255, 0, true},              //  21
	{doNOP, 255, 2, 0, false},                             //  22
	{doSuppressComments, 63 /* ? */, 25, 0, true},         //  23      open-paren-quant
	{doNOP, 255, 27, 0, false},                            //  24
	{doNOP, 35 /* # */, 50, 14, true},                     //  25      open-paren-quant2
	{doNOP, 255, 29, 0, false},                            //  26
	{doSuppressComments, 63 /* ? */, 29, 0, true},         //  27      open-paren
	{doOpenCaptureParen, 255, 2, 14, false},               //  28
	{doOpenNonCaptureParen, 58 /* : */, 2, 14, true},      //  29      open-paren-extended
	{doOpenAtomicParen, 62 /* > */, 2, 14, true},          //  30
	{doOpenLookAhead, 61 /* = */, 2, 20, true},            //  31
	{doOpenLookAheadNeg, 33 /* ! */, 2, 20, true},         //  32
	{doNOP, 60 /* < */, 46, 0, true},                      //  33
	{doNOP, 35 /* # */, 50, 2, true},                      //  34
	{doBeginMatchMode, 105 /* i */, 53, 0, false},         //  35
	{doBeginMatchMode, 100 /* d */, 53, 0, false},         //  36
	{doBeginMatchMode, 109 /* m */, 53, 0, false},         //  37
	{doBeginMatchMode, 115 /* s */, 53, 0, false},         //  38
	{doBeginMatchMode, 117 /* u */, 53, 0, false},         //  39
	{doBeginMatchMode, 119 /* w */, 53, 0, false},         //  40
	{doBeginMatchMode, 120 /* x */, 53, 0, false},         //  41
	{doBeginMatchMode, 45 /* - */, 53, 0, false},          //  42
	{doConditionalExpr, 40 /* ( */, 206, 0, true},         //  43
	{doPerlInline, 123 /* { */, 206, 0, true},             //  44
	{doBadOpenParenType, 255, 206, 0, false},              //  45
	{doOpenLookBehind, 61 /* = */, 2, 20, true},           //  46      open-paren-lookbehind
	{doOpenLookBehindNeg, 33 /* ! */, 2, 20, true},        //  47
	{doBeginNamedCapture, 129, 64, 0, false},              //  48
	{doBadOpenParenType, 255, 206, 0, false},              //  49
	{doNOP, 41 /* ) */, 255, 0, true},                     //  50      paren-comment
	{doMismatchedParenErr, 253, 206, 0, false},            //  51
	{doNOP, 255, 50, 0, true},                             //  52
	{doMatchMode, 105 /* i */, 53, 0, true},               //  53      paren-flag
	{doMatchMode, 100 /* d */, 53, 0, true},               //  54
	{doMatchMode, 109 /* m */, 53, 0, true},               //  55
	{doMatchMode, 115 /* s */, 53, 0, true},               //  56
	{doMatchMode, 117 /* u */, 53, 0, true},               //  57
	{doMatchMode, 119 /* w */, 53, 0, true},               //  58
	{doMatchMode, 120 /* x */, 53, 0, true},               //  59
	{doMatchMode, 45 /* - */, 53, 0, true},                //  60
	{doSetMatchMode, 41 /* ) */, 2, 0, true},              //  61
	{doMatchModeParen, 58 /* : */, 2, 14, true},           //  62
	{doBadModeFlag, 255, 206, 0, false},                   //  63
	{doContinueNamedCapture, 129, 64, 0, true},            //  64      named-capture
	{doContinueNamedCapture, 128, 64, 0, true},            //  65
	{doOpenCaptureParen, 62 /* > */, 2, 14, true},         //  66
	{doBadNamedCapture, 255, 206, 0, false},               //  67
	{doNGStar, 63 /* ? */, 20, 0, true},                   //  68      quant-star
	{doPossessiveStar, 43 /* + */, 20, 0, true},           //  69
	{doStar, 255, 20, 0, false},                           //  70
	{doNGPlus, 63 /* ? */, 20, 0, true},                   //  71      quant-plus
	{doPossessivePlus, 43 /* + */, 20, 0, true},           //  72
	{doPlus, 255, 20, 0, false},                           //  73
	{doNGOpt, 63 /* ? */, 20, 0, true},                    //  74      quant-opt
	{doPossessiveOpt, 43 /* + */, 20, 0, true},            //  75
	{doOpt, 255, 20, 0, false},                            //  76
	{doNOP, 128, 79, 0, false},                            //  77      interval-open
	{doIntervalError, 255, 206, 0, false},                 //  78
	{doIntevalLowerDigit, 128, 79, 0, true},               //  79      interval-lower
	{doNOP, 44 /* , */, 83, 0, true},                      //  80
	{doIntervalSame, 125 /* } */, 86, 0, true},            //  81
	{doIntervalError, 255, 206, 0, false},                 //  82
	{doIntervalUpperDigit, 128, 83, 0, true},              //  83      interval-upper
	{doNOP, 125 /* } */, 86, 0, true},                     //  84
	{doIntervalError, 255, 206, 0, false},                 //  85
	{doNGInterval, 63 /* ? */, 20, 0, true},               //  86      interval-type
	{doPossessiveInterval, 43 /* + */, 20, 0, true},       //  87
	{doInterval, 255, 20, 0, false},                       //  88
	{doBackslashA, 65 /* A */, 2, 0, true},                //  89      backslash
	{doBackslashB, 66 /* B */, 2, 0, true},                //  90
	{doBackslashb, 98 /* b */, 2, 0, true},                //  91
	{doBackslashd, 100 /* d */, 14, 0, true},              //  92
	{doBackslashD, 68 /* D */, 14, 0, true},               //  93
	{doBackslashG, 71 /* G */, 2, 0, true},                //  94
	{doBackslashh, 104 /* h */, 14, 0, true},              //  95
	{doBackslashH, 72 /* H */, 14, 0, true},               //  96
	{doNOP, 107 /* k */, 115, 0, true},                    //  97
	{doNamedChar, 78 /* N */, 14, 0, false},               //  98
	{doProperty, 112 /* p */, 14, 0, false},               //  99
	{doProperty, 80 /* P */, 14, 0, false},                //  100
	{doBackslashR, 82 /* R */, 14, 0, true},               //  101
	{doEnterQuoteMode, 81 /* Q */, 2, 0, true},            //  102
	{doBackslashS, 83 /* S */, 14, 0, true},               //  103
	{doBackslashs, 115 /* s */, 14, 0, true},              //  104
	{doBackslashv, 118 /* v */, 14, 0, true},              //  105
	{doBackslashV, 86 /* V */, 14, 0, true},               //  106
	{doBackslashW, 87 /* W */, 14, 0, true},               //  107
	{doBackslashw, 119 /* w */, 14, 0, true},              //  108
	{doBackslashX, 88 /* X */, 14, 0, true},               //  109
	{doBackslashZ, 90 /* Z */, 2, 0, true},                //  110
	{doBackslashz, 122 /* z */, 2, 0, true},               //  111
	{doBackRef, 128, 14, 0, true},                         //  112
	{doEscapeError, 253, 206, 0, false},                   //  113
	{doEscapedLiteralChar, 255, 14, 0, true},              //  114
	{doBeginNamedBackRef, 60 /* < */, 117, 0, true},       //  115      named-backref
	{doBadNamedCapture, 255, 206, 0, false},               //  116
	{doContinueNamedBackRef, 129, 119, 0, true},           //  117      named-backref-2
	{doBadNamedCapture, 255, 206, 0, false},               //  118
	{doContinueNamedBackRef, 129, 119, 0, true},           //  119      named-backref-3
	{doContinueNamedBackRef, 128, 119, 0, true},           //  120
	{doCompleteNamedBackRef, 62 /* > */, 14, 0, true},     //  121
	{doBadNamedCapture, 255, 206, 0, false},               //  122
	{doSetNegate, 94 /* ^ */, 126, 0, true},               //  123      set-open
	{doSetPosixProp, 58 /* : */, 128, 0, false},           //  124
	{doNOP, 255, 126, 0, false},                           //  125
	{doSetLiteral, 93 /* ] */, 141, 0, true},              //  126      set-open2
	{doNOP, 255, 131, 0, false},                           //  127
	{doSetEnd, 93 /* ] */, 255, 0, true},                  //  128      set-posix
	{doNOP, 58 /* : */, 131, 0, false},                    //  129
	{doRuleError, 255, 206, 0, false},                     //  130
	{doSetEnd, 93 /* ] */, 255, 0, true},                  //  131      set-start
	{doSetBeginUnion, 91 /* [ */, 123, 148, true},         //  132
	{doNOP, 92 /* \ */, 191, 0, true},                     //  133
	{doNOP, 45 /* - */, 137, 0, true},                     //  134
	{doNOP, 38 /* & */, 139, 0, true},                     //  135
	{doSetLiteral, 255, 141, 0, true},                     //  136
	{doRuleError, 45 /* - */, 206, 0, false},              //  137      set-start-dash
	{doSetAddDash, 255, 141, 0, false},                    //  138
	{doRuleError, 38 /* & */, 206, 0, false},              //  139      set-start-amp
	{doSetAddAmp, 255, 141, 0, false},                     //  140
	{doSetEnd, 93 /* ] */, 255, 0, true},                  //  141      set-after-lit
	{doSetBeginUnion, 91 /* [ */, 123, 148, true},         //  142
	{doNOP, 45 /* - */, 178, 0, true},                     //  143
	{doNOP, 38 /* & */, 169, 0, true},                     //  144
	{doNOP, 92 /* \ */, 191, 0, true},                     //  145
	{doSetNoCloseError, 253, 206, 0, false},               //  146
	{doSetLiteral, 255, 141, 0, true},                     //  147
	{doSetEnd, 93 /* ] */, 255, 0, true},                  //  148      set-after-set
	{doSetBeginUnion, 91 /* [ */, 123, 148, true},         //  149
	{doNOP, 45 /* - */, 171, 0, true},                     //  150
	{doNOP, 38 /* & */, 166, 0, true},                     //  151
	{doNOP, 92 /* \ */, 191, 0, true},                     //  152
	{doSetNoCloseError, 253, 206, 0, false},               //  153
	{doSetLiteral, 255, 141, 0, true},                     //  154
	{doSetEnd, 93 /* ] */, 255, 0, true},                  //  155      set-after-range
	{doSetBeginUnion, 91 /* [ */, 123, 148, true},         //  156
	{doNOP, 45 /* - */, 174, 0, true},                     //  157
	{doNOP, 38 /* & */, 176, 0, true},                     //  158
	{doNOP, 92 /* \ */, 191, 0, true},                     //  159
	{doSetNoCloseError, 253, 206, 0, false},               //  160
	{doSetLiteral, 255, 141, 0, true},                     //  161
	{doSetBeginUnion, 91 /* [ */, 123, 148, true},         //  162      set-after-op
	{doSetOpError, 93 /* ] */, 206, 0, false},             //  163
	{doNOP, 92 /* \ */, 191, 0, true},                     //  164
	{doSetLiteral, 255, 141, 0, true},                     //  165
	{doSetBeginIntersection1, 91 /* [ */, 123, 148, true}, //  166      set-set-amp
	{doSetIntersection2, 38 /* & */, 162, 0, true},        //  167
	{doSetAddAmp, 255, 141, 0, false},                     //  168
	{doSetIntersection2, 38 /* & */, 162, 0, true},        //  169      set-lit-amp
	{doSetAddAmp, 255, 141, 0, false},                     //  170
	{doSetBeginDifference1, 91 /* [ */, 123, 148, true},   //  171      set-set-dash
	{doSetDifference2, 45 /* - */, 162, 0, true},          //  172
	{doSetAddDash, 255, 141, 0, false},                    //  173
	{doSetDifference2, 45 /* - */, 162, 0, true},          //  174      set-range-dash
	{doSetAddDash, 255, 141, 0, false},                    //  175
	{doSetIntersection2, 38 /* & */, 162, 0, true},        //  176      set-range-amp
	{doSetAddAmp, 255, 141, 0, false},                     //  177
	{doSetDifference2, 45 /* - */, 162, 0, true},          //  178      set-lit-dash
	{doSetAddDash, 91 /* [ */, 141, 0, false},             //  179
	{doSetAddDash, 93 /* ] */, 141, 0, false},             //  180
	{doNOP, 92 /* \ */, 183, 0, true},                     //  181
	{doSetRange, 255, 155, 0, true},                       //  182
	{doSetOpError, 115 /* s */, 206, 0, false},            //  183      set-lit-dash-escape
	{doSetOpError, 83 /* S */, 206, 0, false},             //  184
	{doSetOpError, 119 /* w */, 206, 0, false},            //  185
	{doSetOpError, 87 /* W */, 206, 0, false},             //  186
	{doSetOpError, 100 /* d */, 206, 0, false},            //  187
	{doSetOpError, 68 /* D */, 206, 0, false},             //  188
	{doSetNamedRange, 78 /* N */, 155, 0, false},          //  189
	{doSetRange, 255, 155, 0, true},                       //  190
	{doSetProp, 112 /* p */, 148, 0, false},               //  191      set-escape
	{doSetProp, 80 /* P */, 148, 0, false},                //  192
	{doSetNamedChar, 78 /* N */, 141, 0, false},           //  193
	{doSetBackslashs, 115 /* s */, 155, 0, true},          //  194
	{doSetBackslashS, 83 /* S */, 155, 0, true},           //  195
	{doSetBackslashw, 119 /* w */, 155, 0, true},          //  196
	{doSetBackslashW, 87 /* W */, 155, 0, true},           //  197
	{doSetBackslashd, 100 /* d */, 155, 0, true},          //  198
	{doSetBackslashD, 68 /* D */, 155, 0, true},           //  199
	{doSetBackslashh, 104 /* h */, 155, 0, true},          //  200
	{doSetBackslashH, 72 /* H */, 155, 0, true},           //  201
	{doSetBackslashv, 118 /* v */, 155, 0, true},          //  202
	{doSetBackslashV, 86 /* V */, 155, 0, true},           //  203
	{doSetLiteralEscaped, 255, 141, 0, true},              //  204
	{doSetFinish, 255, 14, 0, false},                      //  205      set-finish
	{doExit, 255, 206, 0, true},                           //  206      errorDeath
}

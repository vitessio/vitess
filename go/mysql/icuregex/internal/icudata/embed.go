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

package icudata

import _ "embed"

// PNames is the list of property names. It is used
// for example by usage of Unicode propery name aliases
// in regular expressions.
//
//go:embed pnames.icu
var PNames []byte

// UBidi is the list of bidi properties. These are used
// by Bidi class aliases in regular expressions.
//
//go:embed ubidi.icu
var UBidi []byte

// UCase is the list of case properties. These are used
// for case folding internally for case insensitive matching.
//
//go:embed ucase.icu
var UCase []byte

// ULayout is used for property checks agains the InPC, InSC
// and VO properties.
//
//go:embed ulayout.icu
var ULayout []byte

// UNames is used for named character references in regular
// expressions.
//
//go:embed unames.icu
var UNames []byte

// UProps is used for all the character properties. These
// are used to retrieve properties of characters for character
// classes, like letters, whitespace, digits etc.
//
//go:embed uprops.icu
var UProps []byte

// Nfc is the table for character normalization where canonical
// decomposition is done followed by canonical composition.
// This is used for property checks of characters about composition.
//
//go:embed nfc.nrm
var Nfc []byte

// Nfkc is the table for character normalization where compatibility
// decomposition is done followed by canonical composition.
// This is used for property checks of characters about composition.
//
//go:embed nfkc.nrm
var Nfkc []byte

// NfkcCf is the table for character normalization where compatibility
// decomposition is done followed by canonical composition with
// case folding.
// This is used for property checks of characters about composition.
//
//go:embed nfkc_cf.nrm
var NfkcCf []byte

// BrkChar is used for matching against character break
// characters in regular expressions.
//
//go:embed char.brk
var BrkChar []byte

// BrkWord is used for matching against word break
// characters in regular expressions.
//
//go:embed word.brk
var BrkWord []byte

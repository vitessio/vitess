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

package unames

import (
	"sync"

	"vitess.io/vitess/go/mysql/icuregex/internal/icudata"
	"vitess.io/vitess/go/mysql/icuregex/internal/udata"
)

var charNamesOnce sync.Once
var charNames *unames

type unames struct {
	tokens       []uint16
	tokenStrings []uint8
	groups       []uint16
	groupNames   []uint8
	algNames     []algorithmicRange
}

func loadCharNames() {
	charNamesOnce.Do(func() {
		b := udata.NewBytes(icudata.UNames)
		if err := b.ReadHeader(func(info *udata.DataInfo) bool {
			return info.Size >= 20 &&
				info.IsBigEndian == 0 &&
				info.CharsetFamily == 0 &&
				info.DataFormat[0] == 0x75 && /* dataFormat="unam" */
				info.DataFormat[1] == 0x6e &&
				info.DataFormat[2] == 0x61 &&
				info.DataFormat[3] == 0x6d &&
				info.FormatVersion[0] == 1
		}); err != nil {
			panic(err)
		}

		tokenStringOffset := int32(b.Uint32() - 16)
		groupsOffset := int32(b.Uint32() - 16)
		groupStringOffset := int32(b.Uint32() - 16)
		algNamesOffset := int32(b.Uint32() - 16)
		charNames = &unames{
			tokens:       b.Uint16Slice(tokenStringOffset / 2),
			tokenStrings: b.Uint8Slice(groupsOffset - tokenStringOffset),
			groups:       b.Uint16Slice((groupStringOffset - groupsOffset) / 2),
			groupNames:   b.Uint8Slice(algNamesOffset - groupStringOffset),
		}

		algCount := b.Uint32()
		charNames.algNames = make([]algorithmicRange, 0, algCount)

		for i := uint32(0); i < algCount; i++ {
			ar := algorithmicRange{
				start:   b.Uint32(),
				end:     b.Uint32(),
				typ:     b.Uint8(),
				variant: b.Uint8(),
			}
			size := b.Uint16()
			switch ar.typ {
			case 0:
				ar.s = b.Uint8Slice(int32(size) - 12)
			case 1:
				ar.factors = b.Uint16Slice(int32(ar.variant))
				ar.s = b.Uint8Slice(int32(size) - 12 - int32(ar.variant)*2)
			}
			charNames.algNames = append(charNames.algNames, ar)
		}
	})
}

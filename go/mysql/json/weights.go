/*
Copyright 2023 The Vitess Authors.

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

package json

import (
	"encoding/binary"
	"strings"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/fastparse"
)

const (
	JSON_KEY_NULL        = '\x00'
	JSON_KEY_NUMBER_NEG  = '\x01'
	JSON_KEY_NUMBER_ZERO = '\x02'
	JSON_KEY_NUMBER_POS  = '\x03'
	JSON_KEY_STRING      = '\x04'
	JSON_KEY_OBJECT      = '\x05'
	JSON_KEY_ARRAY       = '\x06'
	JSON_KEY_FALSE       = '\x07'
	JSON_KEY_TRUE        = '\x08'
	JSON_KEY_DATE        = '\x09'
	JSON_KEY_TIME        = '\x0A'
	JSON_KEY_DATETIME    = '\x0B'
	JSON_KEY_OPAQUE      = '\x0C'
)

// numericWeightString generates a fixed-width weight string for any JSON
// number. It requires the `num` representation to be normalized, otherwise
// the resulting string will not sort.
func (v *Value) numericWeightString(dst []byte, num string) []byte {
	const MaxPadLength = 30

	var (
		exponent    string
		exp         int64
		significant string
		negative    bool
		original    = len(dst)
	)

	if num[0] == '-' {
		negative = true
		num = num[1:]
	}

	if i := strings.IndexByte(num, 'e'); i >= 0 {
		exponent = num[i+1:]
		num = num[:i]
	}

	significant = num
	for len(significant) > 0 {
		if significant[0] >= '1' && significant[0] <= '9' {
			break
		}
		significant = significant[1:]
	}
	if len(significant) == 0 {
		return append(dst, JSON_KEY_NUMBER_ZERO)
	}

	if len(exponent) > 0 {
		exp, _ = fastparse.ParseInt64(exponent, 10)
	} else {
		dec := strings.IndexByte(num, '.')
		ofs := len(num) - len(significant)
		if dec < 0 {
			exp = int64(len(significant) - 1)
		} else if ofs < dec {
			exp = int64(dec - ofs - 1)
		} else {
			exp = int64(dec - ofs)
		}
	}

	if negative {
		dst = append(dst, JSON_KEY_NUMBER_NEG)
		dst = binary.BigEndian.AppendUint16(dst, uint16(-exp)^(1<<15))

		for _, ch := range []byte(significant) {
			if ch >= '0' && ch <= '9' {
				dst = append(dst, '9'-ch+'0')
			}
		}
		for len(dst)-original < MaxPadLength {
			dst = append(dst, '9')
		}
	} else {
		dst = append(dst, JSON_KEY_NUMBER_POS)
		dst = binary.BigEndian.AppendUint16(dst, uint16(exp)^(1<<15))

		for _, ch := range []byte(significant) {
			if ch >= '0' && ch <= '9' {
				dst = append(dst, ch)
			}
		}
		for len(dst)-original < MaxPadLength {
			dst = append(dst, '0')
		}
	}

	return dst
}

func (v *Value) WeightString(dst []byte) []byte {
	switch v.Type() {
	case TypeNull:
		dst = append(dst, JSON_KEY_NULL)
	case TypeNumber:
		if v.NumberType() == NumberTypeFloat {
			f := v.marshalFloat(nil)
			dst = v.numericWeightString(dst, hack.String(f))
		} else {
			dst = v.numericWeightString(dst, v.s)
		}
	case TypeString:
		dst = append(dst, JSON_KEY_STRING)
		dst = append(dst, v.s...)
	case TypeObject:
		// MySQL compat: we follow the same behavior as MySQL does for weight strings in JSON,
		// where Objects and Arrays are only sorted by their length and not by the values
		// of their contents.
		// Note that in MySQL, generating the weight string of a JSON Object or Array will actually
		// print a warning in the logs! We're not printing anything.
		dst = append(dst, JSON_KEY_OBJECT)
		dst = binary.BigEndian.AppendUint32(dst, uint32(v.o.Len()))
	case TypeArray:
		dst = append(dst, JSON_KEY_ARRAY)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(v.a)))
	case TypeBoolean:
		switch v {
		case ValueTrue:
			dst = append(dst, JSON_KEY_TRUE)
		case ValueFalse:
			dst = append(dst, JSON_KEY_FALSE)
		default:
			panic("invalid JSON Boolean")
		}
	case TypeDate:
		dst = append(dst, JSON_KEY_DATE)
		dst = append(dst, v.MarshalDate()...)
	case TypeDateTime:
		dst = append(dst, JSON_KEY_DATETIME)
		dst = append(dst, v.MarshalDateTime()...)
	case TypeTime:
		dst = append(dst, JSON_KEY_TIME)
		dst = append(dst, v.MarshalTime()...)
	case TypeOpaque, TypeBit, TypeBlob:
		dst = append(dst, JSON_KEY_OPAQUE)
		dst = append(dst, v.s...)
	}
	return dst
}

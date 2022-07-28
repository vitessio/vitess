/*
Copyright 2021 The Vitess Authors.

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

package uca

// UnicodeDecomposeHangulSyllable breaks down a Korean Hangul rune into its 2 or 3 composited
// codepoints.
// This is a straight port of the algorithm in http://www.unicode.org/versions/Unicode9.0.0/ch03.pdf
func UnicodeDecomposeHangulSyllable(syl rune) []rune {
	const baseSyllabe = 0xAC00
	const baseLeadingJamo = 0x1100
	const baseVowelJamo = 0x1161
	const baseTrailingJamo = 0x11A7
	const countVowelJamo = 21
	const countTrailingJamo = 28
	const countJamoCombinations = countVowelJamo * countTrailingJamo

	if syl < 0xAC00 || syl > 0xD7AF {
		return nil
	}

	sylIndex := syl - baseSyllabe
	indexLeadingJamo := sylIndex / countJamoCombinations
	indexVowelJamo := (sylIndex % countJamoCombinations) / countTrailingJamo

	result := []rune{baseLeadingJamo + indexLeadingJamo, baseVowelJamo + indexVowelJamo}
	if indexTrailingJamo := sylIndex % countTrailingJamo; indexTrailingJamo != 0 {
		result = append(result, baseTrailingJamo+indexTrailingJamo)
	}
	return result
}

// UnicodeImplicitWeights900 generates the implicit weights for this codepoint.
// This is a straight port of the algorithm in https://www.unicode.org/reports/tr10/tr10-34.html#Implicit_Weights
// It only applies to the UCA Standard v9.0.0
func UnicodeImplicitWeights900(weights []uint16, codepoint rune) {
	var aaaa, bbbb uint16

	switch {
	case codepoint >= 0x17000 && codepoint <= 0x18AFF:
		aaaa = 0xFB00
		bbbb = uint16(codepoint-0x17000) | 0x8000

	case (codepoint >= 0x4E00 && codepoint <= 0x9FD5) ||
		(codepoint >= 0xFA0E && codepoint <= 0xFA29):
		aaaa = 0xFB40 + uint16(codepoint>>15)
		bbbb = uint16(codepoint&0x7FFF) | 0x8000

	case (codepoint >= 0x3400 && codepoint <= 0x4DB5) ||
		(codepoint >= 0x20000 && codepoint <= 0x2A6D6) ||
		(codepoint >= 0x2A700 && codepoint <= 0x2B734) ||
		(codepoint >= 0x2B740 && codepoint <= 0x2B81D) ||
		(codepoint >= 0x2B820 && codepoint <= 0x2CEA1):
		aaaa = 0xFB80 + uint16(codepoint>>15)
		bbbb = uint16(codepoint&0x7FFF) | 0x8000

	default:
		aaaa = 0xFBC0 + uint16(codepoint>>15)
		bbbb = uint16(codepoint&0x7FFF) | 0x8000
	}

	weights[0] = aaaa
	weights[1] = 0x0020
	weights[2] = 0x0002
	weights[3] = bbbb
	weights[4] = 0x0000
	weights[5] = 0x0000
}

// UnicodeImplicitWeightsLegacy generates the implicit weights for this codepoint.
// This is a straight port of the algorithm in https://www.unicode.org/reports/tr10/tr10-20.html#Implicit_Weights
// It only applies to the UCA Standard v4.0.0 and v5.2.0
func UnicodeImplicitWeightsLegacy(weights []uint16, codepoint rune) {
	/*
		To derive the collation elements, the value of the code point is used to calculate two numbers,
		by bit shifting and bit masking. The bit operations are chosen so that the resultant numbers have
		the desired ranges for constructing implicit weights. The first number is calculated by taking the
		code point expressed as a 32-bit binary integer CP and bit shifting it right by 15 bits.
		Because code points range from U+0000 to U+10FFFF, the result will be a number in the range 0 to 2116 (= 3310).
		This number is then added to the special value BASE.

			AAAA = BASE + (CP >> 15);

		Now mask off the bottom 15 bits of CP. OR a 1 into bit 15, so that the resultant value is non-zero.

			BBBB = (CP & 0x7FFF) | 0x8000;

		AAAA and BBBB are interpreted as unsigned 16-bit integers. The implicit weight mapping given to
		the code point is then constructed as:

			[.AAAA.0020.0002.][.BBBB.0000.0000.]

		However, note that for the legacy iterator, to match MySQL's behavior, we're only
		iterating through the level 0 weights, so we only have to yield AAAA and BBBB.
	*/
	switch {
	case codepoint >= 0x3400 && codepoint <= 0x4DB5:
		weights[0] = 0xFB80 + uint16(codepoint>>15)
	case codepoint >= 0x4E00 && codepoint <= 0x9FA5:
		weights[0] = 0xFB40 + uint16(codepoint>>15)
	default:
		weights[0] = 0xFBC0 + uint16(codepoint>>15)
	}

	weights[1] = uint16(codepoint&0x7FFF) | 0x8000
}

func unicodeIsKatakana(cp rune) bool {
	switch {
	case cp == 0x30FD || cp == 0x30FE || cp == 0x30FC:
		return true
	case (cp >= 0x30A1 && cp <= 0x30FA) || (cp >= 0xFF66 && cp <= 0xFF9D):
		return true
	default:
		return false
	}
}

func unicodeIsHiragana(cp rune) bool {
	switch {
	case cp >= 0x3041 && cp <= 0x3096:
		return true
	case cp == 0x309D || cp == 0x309E:
		return true
	default:
		return false
	}
}

// unicodeImplicitChineseWeights adjusts the ordering weights for implicit
// codepoints in Chinese collations. It is not clear what is the rationale
// behind these adjustments to the AAAA page for the weights, but these
// page offsets have been reverse engineered to pass the WEIGHT_STRING tests
// for all codepoints in the ZH range
//
// TODO: is this the right level to perform the adjustment?
func unicodeImplicitChineseWeights(weights []uint16, codepoint rune) {
	UnicodeImplicitWeights900(weights, codepoint)

	switch weights[0] {
	case 0xFB00:
		weights[0] -= 1247
	case 0xFB40, 0xFB41:
		weights[0] -= 15745
	case 0xFB80:
		weights[0] -= 15807
	case 0xFB84, 0xFB85:
		weights[0] -= 15810
	default:
		weights[0] -= 1438
	}
}

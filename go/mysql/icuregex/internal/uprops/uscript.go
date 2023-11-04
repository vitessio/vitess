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

package uprops

import "vitess.io/vitess/go/mysql/icuregex/internal/uchar"

/**
 * Constants for ISO 15924 script codes.
 *
 * The current set of script code constants supports at least all scripts
 * that are encoded in the version of Unicode which ICU currently supports.
 * The names of the constants are usually derived from the
 * Unicode script property value aliases.
 * See UAX #24 Unicode Script Property (http://www.unicode.org/reports/tr24/)
 * and http://www.unicode.org/Public/UCD/latest/ucd/PropertyValueAliases.txt .
 *
 * In addition, constants for many ISO 15924 script codes
 * are included, for use with language tags, CLDR data, and similar.
 * Some of those codes are not used in the Unicode Character Database (UCD).
 * For example, there are no characters that have a UCD script property value of
 * Hans or Hant. All Han ideographs have the Hani script property value in Unicode.
 *
 * Private-use codes Qaaa..Qabx are not included, except as used in the UCD or in CLDR.
 *
 * Starting with ICU 55, script codes are only added when their scripts
 * have been or will certainly be encoded in Unicode,
 * and have been assigned Unicode script property value aliases,
 * to ensure that their script names are stable and match the names of the constants.
 * Script codes like Latf and Aran that are not subject to separate encoding
 * may be added at any time.
 *
 * @stable ICU 2.2
 */
type code int32

/*
 * Note: UScriptCode constants and their ISO script code comments
 * are parsed by preparseucd.py.
 * It matches lines like
 *     USCRIPT_<Unicode Script value name> = <integer>,  / * <ISO script code> * /
 */

const (
	/** @stable ICU 2.2 */
	invalidCode code = -1
	/** @stable ICU 2.2 */
	common code = 0 /* Zyyy */
	/** @stable ICU 2.2 */
	inherited code = 1 /* Zinh */ /* "Code for inherited script", for non-spacing combining marks; also Qaai */
	/** @stable ICU 2.2 */
	arabic code = 2 /* Arab */
	/** @stable ICU 2.2 */
	armenian code = 3 /* Armn */
	/** @stable ICU 2.2 */
	bengali code = 4 /* Beng */
	/** @stable ICU 2.2 */
	bopomofo code = 5 /* Bopo */
	/** @stable ICU 2.2 */
	cherokee code = 6 /* Cher */
	/** @stable ICU 2.2 */
	coptic code = 7 /* Copt */
	/** @stable ICU 2.2 */
	cyrillic code = 8 /* Cyrl */
	/** @stable ICU 2.2 */
	deseret code = 9 /* Dsrt */
	/** @stable ICU 2.2 */
	devanagari code = 10 /* Deva */
	/** @stable ICU 2.2 */
	ethiopic code = 11 /* Ethi */
	/** @stable ICU 2.2 */
	georgian code = 12 /* Geor */
	/** @stable ICU 2.2 */
	gothic code = 13 /* Goth */
	/** @stable ICU 2.2 */
	greek code = 14 /* Grek */
	/** @stable ICU 2.2 */
	gujarati code = 15 /* Gujr */
	/** @stable ICU 2.2 */
	gurmukhi code = 16 /* Guru */
	/** @stable ICU 2.2 */
	han code = 17 /* Hani */
	/** @stable ICU 2.2 */
	hangul code = 18 /* Hang */
	/** @stable ICU 2.2 */
	hebrew code = 19 /* Hebr */
	/** @stable ICU 2.2 */
	hiragana code = 20 /* Hira */
	/** @stable ICU 2.2 */
	kannada code = 21 /* Knda */
	/** @stable ICU 2.2 */
	katakana code = 22 /* Kana */
	/** @stable ICU 2.2 */
	khmer code = 23 /* Khmr */
	/** @stable ICU 2.2 */
	lao code = 24 /* Laoo */
	/** @stable ICU 2.2 */
	latin code = 25 /* Latn */
	/** @stable ICU 2.2 */
	malayalam code = 26 /* Mlym */
	/** @stable ICU 2.2 */
	mongolian code = 27 /* Mong */
	/** @stable ICU 2.2 */
	myanmar code = 28 /* Mymr */
	/** @stable ICU 2.2 */
	ogham code = 29 /* Ogam */
	/** @stable ICU 2.2 */
	oldItalic code = 30 /* Ital */
	/** @stable ICU 2.2 */
	oriya code = 31 /* Orya */
	/** @stable ICU 2.2 */
	runic code = 32 /* Runr */
	/** @stable ICU 2.2 */
	sinhala code = 33 /* Sinh */
	/** @stable ICU 2.2 */
	syriac code = 34 /* Syrc */
	/** @stable ICU 2.2 */
	tamil code = 35 /* Taml */
	/** @stable ICU 2.2 */
	telugu code = 36 /* Telu */
	/** @stable ICU 2.2 */
	thaana code = 37 /* Thaa */
	/** @stable ICU 2.2 */
	thai code = 38 /* Thai */
	/** @stable ICU 2.2 */
	tibetan code = 39 /* Tibt */
	/** Canadian_Aboriginal script. @stable ICU 2.6 */
	canadianAboriginal code = 40 /* Cans */
	/** Canadian_Aboriginal script (alias). @stable ICU 2.2 */
	ucas code = canadianAboriginal
	/** @stable ICU 2.2 */
	yi code = 41 /* Yiii */
	/* New scripts in Unicode 3.2 */
	/** @stable ICU 2.2 */
	tagalog code = 42 /* Tglg */
	/** @stable ICU 2.2 */
	hanunoo code = 43 /* Hano */
	/** @stable ICU 2.2 */
	buhid code = 44 /* Buhd */
	/** @stable ICU 2.2 */
	tagbanwa code = 45 /* Tagb */

	/* New scripts in Unicode 4 */
	/** @stable ICU 2.6 */
	braille code = 46 /* Brai */
	/** @stable ICU 2.6 */
	cypriot code = 47 /* Cprt */
	/** @stable ICU 2.6 */
	limbu code = 48 /* Limb */
	/** @stable ICU 2.6 */
	linearB code = 49 /* Linb */
	/** @stable ICU 2.6 */
	osmanya code = 50 /* Osma */
	/** @stable ICU 2.6 */
	shavian code = 51 /* Shaw */
	/** @stable ICU 2.6 */
	taiLe code = 52 /* Tale */
	/** @stable ICU 2.6 */
	ugaratic code = 53 /* Ugar */

	/** New script code in Unicode 4.0.1 @stable ICU 3.0 */
	katakanaOrHiragana = 54 /*Hrkt */

	/* New scripts in Unicode 4.1 */
	/** @stable ICU 3.4 */
	buginese code = 55 /* Bugi */
	/** @stable ICU 3.4 */
	glagolitic code = 56 /* Glag */
	/** @stable ICU 3.4 */
	kharoshthi code = 57 /* Khar */
	/** @stable ICU 3.4 */
	sylotiNagri code = 58 /* Sylo */
	/** @stable ICU 3.4 */
	newTaiLue code = 59 /* Talu */
	/** @stable ICU 3.4 */
	tifinagh code = 60 /* Tfng */
	/** @stable ICU 3.4 */
	oldPersian code = 61 /* Xpeo */

	/* New script codes from Unicode and ISO 15924 */
	/** @stable ICU 3.6 */
	balinese code = 62 /* Bali */
	/** @stable ICU 3.6 */
	batak code = 63 /* Batk */
	/** @stable ICU 3.6 */
	blissymbols code = 64 /* Blis */
	/** @stable ICU 3.6 */
	brahmi code = 65 /* Brah */
	/** @stable ICU 3.6 */
	cham code = 66 /* Cham */
	/** @stable ICU 3.6 */
	cirth code = 67 /* Cirt */
	/** @stable ICU 3.6 */
	oldChurchSlavonicCyrillic code = 68 /* Cyrs */
	/** @stable ICU 3.6 */
	demoticEgyptian code = 69 /* Egyd */
	/** @stable ICU 3.6 */
	hieraticEgyptian code = 70 /* Egyh */
	/** @stable ICU 3.6 */
	egyptianHieroglyphs code = 71 /* Egyp */
	/** @stable ICU 3.6 */
	khutsuri code = 72 /* Geok */
	/** @stable ICU 3.6 */
	simplfiedHan code = 73 /* Hans */
	/** @stable ICU 3.6 */
	traditionalHan code = 74 /* Hant */
	/** @stable ICU 3.6 */
	pahawhHmong code = 75 /* Hmng */
	/** @stable ICU 3.6 */
	oldHungarian code = 76 /* Hung */
	/** @stable ICU 3.6 */
	harappanIndus code = 77 /* Inds */
	/** @stable ICU 3.6 */
	javanese code = 78 /* Java */
	/** @stable ICU 3.6 */
	kayahLi code = 79 /* Kali */
	/** @stable ICU 3.6 */
	latinFraktur code = 80 /* Latf */
	/** @stable ICU 3.6 */
	latinGaelic code = 81 /* Latg */
	/** @stable ICU 3.6 */
	lepcha code = 82 /* Lepc */
	/** @stable ICU 3.6 */
	linearA code = 83 /* Lina */
	/** @stable ICU 4.6 */
	mandaic code = 84 /* Mand */
	/** @stable ICU 3.6 */
	mandaean code = mandaic
	/** @stable ICU 3.6 */
	mayanHieroglyphs code = 85 /* Maya */
	/** @stable ICU 4.6 */
	meroiticHieroglyphs code = 86 /* Mero */
	/** @stable ICU 3.6 */
	meroitic code = meroiticHieroglyphs
	/** @stable ICU 3.6 */
	nko code = 87 /* Nkoo */
	/** @stable ICU 3.6 */
	orkhon code = 88 /* Orkh */
	/** @stable ICU 3.6 */
	oldPermic code = 89 /* Perm */
	/** @stable ICU 3.6 */
	phagsPa code = 90 /* Phag */
	/** @stable ICU 3.6 */
	phoenician code = 91 /* Phnx */
	/** @stable ICU 52 */
	miao code = 92 /* Plrd */
	/** @stable ICU 3.6 */
	phoneticPollard code = miao
	/** @stable ICU 3.6 */
	rongoRongo code = 93 /* Roro */
	/** @stable ICU 3.6 */
	sarati code = 94 /* Sara */
	/** @stable ICU 3.6 */
	extrangeloSyriac code = 95 /* Syre */
	/** @stable ICU 3.6 */
	westernSyriac code = 96 /* Syrj */
	/** @stable ICU 3.6 */
	easternSyriac code = 97 /* Syrn */
	/** @stable ICU 3.6 */
	tengwar code = 98 /* Teng */
	/** @stable ICU 3.6 */
	vai code = 99 /* Vaii */
	/** @stable ICU 3.6 */
	visibleSpeech code = 100 /* Visp */
	/** @stable ICU 3.6 */
	cuneiform code = 101 /* Xsux */
	/** @stable ICU 3.6 */
	unwrittenLanguages code = 102 /* Zxxx */
	/** @stable ICU 3.6 */
	unknown code = 103 /* Zzzz */ /* Unknown="Code for uncoded script", for unassigned code points */

	/** @stable ICU 3.8 */
	carian code = 104 /* Cari */
	/** @stable ICU 3.8 */
	japanese code = 105 /* Jpan */
	/** @stable ICU 3.8 */
	lanna code = 106 /* Lana */
	/** @stable ICU 3.8 */
	lycian code = 107 /* Lyci */
	/** @stable ICU 3.8 */
	lydian code = 108 /* Lydi */
	/** @stable ICU 3.8 */
	olChiki code = 109 /* Olck */
	/** @stable ICU 3.8 */
	rejang code = 110 /* Rjng */
	/** @stable ICU 3.8 */
	saurashtra code = 111 /* Saur */
	/** Sutton SignWriting @stable ICU 3.8 */
	signWriting code = 112 /* Sgnw */
	/** @stable ICU 3.8 */
	sundanese code = 113 /* Sund */
	/** @stable ICU 3.8 */
	moon code = 114 /* Moon */
	/** @stable ICU 3.8 */
	meiteiMayek code = 115 /* Mtei */

	/** @stable ICU 4.0 */
	imperialAramaic code = 116 /* Armi */
	/** @stable ICU 4.0 */
	avestan code = 117 /* Avst */
	/** @stable ICU 4.0 */
	chakma code = 118 /* Cakm */
	/** @stable ICU 4.0 */
	korean code = 119 /* Kore */
	/** @stable ICU 4.0 */
	kaithi code = 120 /* Kthi */
	/** @stable ICU 4.0 */
	manichaean code = 121 /* Mani */
	/** @stable ICU 4.0 */
	inscriptionalPahlavi code = 122 /* Phli */
	/** @stable ICU 4.0 */
	psalterPahlavi code = 123 /* Phlp */
	/** @stable ICU 4.0 */
	bookPahlavi code = 124 /* Phlv */
	/** @stable ICU 4.0 */
	inscriptionalParthian code = 125 /* Prti */
	/** @stable ICU 4.0 */
	samaritan code = 126 /* Samr */
	/** @stable ICU 4.0 */
	taiViet code = 127 /* Tavt */
	/** @stable ICU 4.0 */
	mathematicalNotation code = 128 /* Zmth */
	/** @stable ICU 4.0 */
	symbols code = 129 /* Zsym */

	/** @stable ICU 4.4 */
	bamum code = 130 /* Bamu */
	/** @stable ICU 4.4 */
	lisu code = 131 /* Lisu */
	/** @stable ICU 4.4 */
	nakhiGeba code = 132 /* Nkgb */
	/** @stable ICU 4.4 */
	oldSouthArabian code = 133 /* Sarb */

	/** @stable ICU 4.6 */
	bassaVah code = 134 /* Bass */
	/** @stable ICU 54 */
	duployan code = 135 /* Dupl */
	/** @stable ICU 4.6 */
	elbasan code = 136 /* Elba */
	/** @stable ICU 4.6 */
	grantha code = 137 /* Gran */
	/** @stable ICU 4.6 */
	kpelle code = 138 /* Kpel */
	/** @stable ICU 4.6 */
	loma code = 139 /* Loma */
	/** Mende Kikakui @stable ICU 4.6 */
	mende code = 140 /* Mend */
	/** @stable ICU 4.6 */
	meroiticCursive code = 141 /* Merc */
	/** @stable ICU 4.6 */
	oldNorthArabian code = 142 /* Narb */
	/** @stable ICU 4.6 */
	nabataean code = 143 /* Nbat */
	/** @stable ICU 4.6 */
	palmyrene code = 144 /* Palm */
	/** @stable ICU 54 */
	khudawadi code = 145 /* Sind */
	/** @stable ICU 4.6 */
	sindhi code = khudawadi
	/** @stable ICU 4.6 */
	warangCiti code = 146 /* Wara */

	/** @stable ICU 4.8 */
	afaka code = 147 /* Afak */
	/** @stable ICU 4.8 */
	jurchen code = 148 /* Jurc */
	/** @stable ICU 4.8 */
	mro code = 149 /* Mroo */
	/** @stable ICU 4.8 */
	nushu code = 150 /* Nshu */
	/** @stable ICU 4.8 */
	sharada code = 151 /* Shrd */
	/** @stable ICU 4.8 */
	soraSompeng code = 152 /* Sora */
	/** @stable ICU 4.8 */
	takri code = 153 /* Takr */
	/** @stable ICU 4.8 */
	tangut code = 154 /* Tang */
	/** @stable ICU 4.8 */
	woleai code = 155 /* Wole */

	/** @stable ICU 49 */
	anatolianHieroglyphs code = 156 /* Hluw */
	/** @stable ICU 49 */
	khojki code = 157 /* Khoj */
	/** @stable ICU 49 */
	tirhuta code = 158 /* Tirh */

	/** @stable ICU 52 */
	caucasianAlbanian code = 159 /* Aghb */
	/** @stable ICU 52 */
	mahajani code = 160 /* Mahj */

	/** @stable ICU 54 */
	ahom code = 161 /* Ahom */
	/** @stable ICU 54 */
	hatran code = 162 /* Hatr */
	/** @stable ICU 54 */
	modi code = 163 /* Modi */
	/** @stable ICU 54 */
	multani code = 164 /* Mult */
	/** @stable ICU 54 */
	pauCinHau code = 165 /* Pauc */
	/** @stable ICU 54 */
	siddham code = 166 /* Sidd */

	/** @stable ICU 58 */
	adlam code = 167 /* Adlm */
	/** @stable ICU 58 */
	bhaiksuki code = 168 /* Bhks */
	/** @stable ICU 58 */
	marchen code = 169 /* Marc */
	/** @stable ICU 58 */
	newa code = 170 /* Newa */
	/** @stable ICU 58 */
	osage code = 171 /* Osge */

	/** @stable ICU 58 */
	hanWithBopomofo code = 172 /* Hanb */
	/** @stable ICU 58 */
	jamo code = 173 /* Jamo */
	/** @stable ICU 58 */
	symbolsEmoji code = 174 /* Zsye */

	/** @stable ICU 60 */
	masaramGondi code = 175 /* Gonm */
	/** @stable ICU 60 */
	soyombo code = 176 /* Soyo */
	/** @stable ICU 60 */
	zanabazarSquare code = 177 /* Zanb */

	/** @stable ICU 62 */
	dogra code = 178 /* Dogr */
	/** @stable ICU 62 */
	gunjalaGondi code = 179 /* Gong */
	/** @stable ICU 62 */
	makasar code = 180 /* Maka */
	/** @stable ICU 62 */
	medefaidrin code = 181 /* Medf */
	/** @stable ICU 62 */
	hanifiRohingya code = 182 /* Rohg */
	/** @stable ICU 62 */
	sogdian code = 183 /* Sogd */
	/** @stable ICU 62 */
	oldSogdian code = 184 /* Sogo */

	/** @stable ICU 64 */
	elymaic code = 185 /* Elym */
	/** @stable ICU 64 */
	nyiakengPuachueHmong code = 186 /* Hmnp */
	/** @stable ICU 64 */
	nandinagari code = 187 /* Nand */
	/** @stable ICU 64 */
	wancho code = 188 /* Wcho */

	/** @stable ICU 66 */
	chorasmian code = 189 /* Chrs */
	/** @stable ICU 66 */
	divesAkuru code = 190 /* Diak */
	/** @stable ICU 66 */
	khitanSmallScript code = 191 /* Kits */
	/** @stable ICU 66 */
	yezedi code = 192 /* Yezi */
)

func uscriptHasScript(c rune, sc code) bool {
	scriptX := uchar.GetUnicodeProperties(c, 0) & scriptXMask
	codeOrIndex := mergeScriptCodeOrIndex(scriptX)
	if scriptX < scriptXWithCommon {
		return sc == code(codeOrIndex)
	}

	scx := uchar.ScriptExtensions(codeOrIndex)
	if scriptX >= scriptXWithOther {
		scx = uchar.ScriptExtensions(uint32(scx[1]))
	}
	sc32 := uint32(sc)
	if sc32 > 0x7fff {
		/* Guard against bogus input that would make us go past the Script_Extensions terminator. */
		return false
	}
	for sc32 > uint32(scx[0]) {
		scx = scx[1:]
	}
	return sc32 == uint32(scx[0]&0x7fff)
}

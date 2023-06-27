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

import (
	"vitess.io/vitess/go/mysql/icuregex/internal/uchar"
)

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
type UScriptCode int32

/*
 * Note: UScriptCode constants and their ISO script code comments
 * are parsed by preparseucd.py.
 * It matches lines like
 *     USCRIPT_<Unicode Script value name> = <integer>,  / * <ISO script code> * /
 */

const (
	/** @stable ICU 2.2 */
	USCRIPT_INVALID_CODE UScriptCode = -1
	/** @stable ICU 2.2 */
	USCRIPT_COMMON UScriptCode = 0 /* Zyyy */
	/** @stable ICU 2.2 */
	USCRIPT_INHERITED UScriptCode = 1 /* Zinh */ /* "Code for inherited script", for non-spacing combining marks; also Qaai */
	/** @stable ICU 2.2 */
	USCRIPT_ARABIC UScriptCode = 2 /* Arab */
	/** @stable ICU 2.2 */
	USCRIPT_ARMENIAN UScriptCode = 3 /* Armn */
	/** @stable ICU 2.2 */
	USCRIPT_BENGALI UScriptCode = 4 /* Beng */
	/** @stable ICU 2.2 */
	USCRIPT_BOPOMOFO UScriptCode = 5 /* Bopo */
	/** @stable ICU 2.2 */
	USCRIPT_CHEROKEE UScriptCode = 6 /* Cher */
	/** @stable ICU 2.2 */
	USCRIPT_COPTIC UScriptCode = 7 /* Copt */
	/** @stable ICU 2.2 */
	USCRIPT_CYRILLIC UScriptCode = 8 /* Cyrl */
	/** @stable ICU 2.2 */
	USCRIPT_DESERET UScriptCode = 9 /* Dsrt */
	/** @stable ICU 2.2 */
	USCRIPT_DEVANAGARI UScriptCode = 10 /* Deva */
	/** @stable ICU 2.2 */
	USCRIPT_ETHIOPIC UScriptCode = 11 /* Ethi */
	/** @stable ICU 2.2 */
	USCRIPT_GEORGIAN UScriptCode = 12 /* Geor */
	/** @stable ICU 2.2 */
	USCRIPT_GOTHIC UScriptCode = 13 /* Goth */
	/** @stable ICU 2.2 */
	USCRIPT_GREEK UScriptCode = 14 /* Grek */
	/** @stable ICU 2.2 */
	USCRIPT_GUJARATI UScriptCode = 15 /* Gujr */
	/** @stable ICU 2.2 */
	USCRIPT_GURMUKHI UScriptCode = 16 /* Guru */
	/** @stable ICU 2.2 */
	USCRIPT_HAN UScriptCode = 17 /* Hani */
	/** @stable ICU 2.2 */
	USCRIPT_HANGUL UScriptCode = 18 /* Hang */
	/** @stable ICU 2.2 */
	USCRIPT_HEBREW UScriptCode = 19 /* Hebr */
	/** @stable ICU 2.2 */
	USCRIPT_HIRAGANA UScriptCode = 20 /* Hira */
	/** @stable ICU 2.2 */
	USCRIPT_KANNADA UScriptCode = 21 /* Knda */
	/** @stable ICU 2.2 */
	USCRIPT_KATAKANA UScriptCode = 22 /* Kana */
	/** @stable ICU 2.2 */
	USCRIPT_KHMER UScriptCode = 23 /* Khmr */
	/** @stable ICU 2.2 */
	USCRIPT_LAO UScriptCode = 24 /* Laoo */
	/** @stable ICU 2.2 */
	USCRIPT_LATIN UScriptCode = 25 /* Latn */
	/** @stable ICU 2.2 */
	USCRIPT_MALAYALAM UScriptCode = 26 /* Mlym */
	/** @stable ICU 2.2 */
	USCRIPT_MONGOLIAN UScriptCode = 27 /* Mong */
	/** @stable ICU 2.2 */
	USCRIPT_MYANMAR UScriptCode = 28 /* Mymr */
	/** @stable ICU 2.2 */
	USCRIPT_OGHAM UScriptCode = 29 /* Ogam */
	/** @stable ICU 2.2 */
	USCRIPT_OLD_ITALIC UScriptCode = 30 /* Ital */
	/** @stable ICU 2.2 */
	USCRIPT_ORIYA UScriptCode = 31 /* Orya */
	/** @stable ICU 2.2 */
	USCRIPT_RUNIC UScriptCode = 32 /* Runr */
	/** @stable ICU 2.2 */
	USCRIPT_SINHALA UScriptCode = 33 /* Sinh */
	/** @stable ICU 2.2 */
	USCRIPT_SYRIAC UScriptCode = 34 /* Syrc */
	/** @stable ICU 2.2 */
	USCRIPT_TAMIL UScriptCode = 35 /* Taml */
	/** @stable ICU 2.2 */
	USCRIPT_TELUGU UScriptCode = 36 /* Telu */
	/** @stable ICU 2.2 */
	USCRIPT_THAANA UScriptCode = 37 /* Thaa */
	/** @stable ICU 2.2 */
	USCRIPT_THAI UScriptCode = 38 /* Thai */
	/** @stable ICU 2.2 */
	USCRIPT_TIBETAN UScriptCode = 39 /* Tibt */
	/** Canadian_Aboriginal script. @stable ICU 2.6 */
	USCRIPT_CANADIAN_ABORIGINAL UScriptCode = 40 /* Cans */
	/** Canadian_Aboriginal script (alias). @stable ICU 2.2 */
	USCRIPT_UCAS UScriptCode = USCRIPT_CANADIAN_ABORIGINAL
	/** @stable ICU 2.2 */
	USCRIPT_YI UScriptCode = 41 /* Yiii */
	/* New scripts in Unicode 3.2 */
	/** @stable ICU 2.2 */
	USCRIPT_TAGALOG UScriptCode = 42 /* Tglg */
	/** @stable ICU 2.2 */
	USCRIPT_HANUNOO UScriptCode = 43 /* Hano */
	/** @stable ICU 2.2 */
	USCRIPT_BUHID UScriptCode = 44 /* Buhd */
	/** @stable ICU 2.2 */
	USCRIPT_TAGBANWA UScriptCode = 45 /* Tagb */

	/* New scripts in Unicode 4 */
	/** @stable ICU 2.6 */
	USCRIPT_BRAILLE UScriptCode = 46 /* Brai */
	/** @stable ICU 2.6 */
	USCRIPT_CYPRIOT UScriptCode = 47 /* Cprt */
	/** @stable ICU 2.6 */
	USCRIPT_LIMBU UScriptCode = 48 /* Limb */
	/** @stable ICU 2.6 */
	USCRIPT_LINEAR_B UScriptCode = 49 /* Linb */
	/** @stable ICU 2.6 */
	USCRIPT_OSMANYA UScriptCode = 50 /* Osma */
	/** @stable ICU 2.6 */
	USCRIPT_SHAVIAN UScriptCode = 51 /* Shaw */
	/** @stable ICU 2.6 */
	USCRIPT_TAI_LE UScriptCode = 52 /* Tale */
	/** @stable ICU 2.6 */
	USCRIPT_UGARITIC UScriptCode = 53 /* Ugar */

	/** New script code in Unicode 4.0.1 @stable ICU 3.0 */
	USCRIPT_KATAKANA_OR_HIRAGANA = 54 /*Hrkt */

	/* New scripts in Unicode 4.1 */
	/** @stable ICU 3.4 */
	USCRIPT_BUGINESE UScriptCode = 55 /* Bugi */
	/** @stable ICU 3.4 */
	USCRIPT_GLAGOLITIC UScriptCode = 56 /* Glag */
	/** @stable ICU 3.4 */
	USCRIPT_KHAROSHTHI UScriptCode = 57 /* Khar */
	/** @stable ICU 3.4 */
	USCRIPT_SYLOTI_NAGRI UScriptCode = 58 /* Sylo */
	/** @stable ICU 3.4 */
	USCRIPT_NEW_TAI_LUE UScriptCode = 59 /* Talu */
	/** @stable ICU 3.4 */
	USCRIPT_TIFINAGH UScriptCode = 60 /* Tfng */
	/** @stable ICU 3.4 */
	USCRIPT_OLD_PERSIAN UScriptCode = 61 /* Xpeo */

	/* New script codes from Unicode and ISO 15924 */
	/** @stable ICU 3.6 */
	USCRIPT_BALINESE UScriptCode = 62 /* Bali */
	/** @stable ICU 3.6 */
	USCRIPT_BATAK UScriptCode = 63 /* Batk */
	/** @stable ICU 3.6 */
	USCRIPT_BLISSYMBOLS UScriptCode = 64 /* Blis */
	/** @stable ICU 3.6 */
	USCRIPT_BRAHMI UScriptCode = 65 /* Brah */
	/** @stable ICU 3.6 */
	USCRIPT_CHAM UScriptCode = 66 /* Cham */
	/** @stable ICU 3.6 */
	USCRIPT_CIRTH UScriptCode = 67 /* Cirt */
	/** @stable ICU 3.6 */
	USCRIPT_OLD_CHURCH_SLAVONIC_CYRILLIC UScriptCode = 68 /* Cyrs */
	/** @stable ICU 3.6 */
	USCRIPT_DEMOTIC_EGYPTIAN UScriptCode = 69 /* Egyd */
	/** @stable ICU 3.6 */
	USCRIPT_HIERATIC_EGYPTIAN UScriptCode = 70 /* Egyh */
	/** @stable ICU 3.6 */
	USCRIPT_EGYPTIAN_HIEROGLYPHS UScriptCode = 71 /* Egyp */
	/** @stable ICU 3.6 */
	USCRIPT_KHUTSURI UScriptCode = 72 /* Geok */
	/** @stable ICU 3.6 */
	USCRIPT_SIMPLIFIED_HAN UScriptCode = 73 /* Hans */
	/** @stable ICU 3.6 */
	USCRIPT_TRADITIONAL_HAN UScriptCode = 74 /* Hant */
	/** @stable ICU 3.6 */
	USCRIPT_PAHAWH_HMONG UScriptCode = 75 /* Hmng */
	/** @stable ICU 3.6 */
	USCRIPT_OLD_HUNGARIAN UScriptCode = 76 /* Hung */
	/** @stable ICU 3.6 */
	USCRIPT_HARAPPAN_INDUS UScriptCode = 77 /* Inds */
	/** @stable ICU 3.6 */
	USCRIPT_JAVANESE UScriptCode = 78 /* Java */
	/** @stable ICU 3.6 */
	USCRIPT_KAYAH_LI UScriptCode = 79 /* Kali */
	/** @stable ICU 3.6 */
	USCRIPT_LATIN_FRAKTUR UScriptCode = 80 /* Latf */
	/** @stable ICU 3.6 */
	USCRIPT_LATIN_GAELIC UScriptCode = 81 /* Latg */
	/** @stable ICU 3.6 */
	USCRIPT_LEPCHA UScriptCode = 82 /* Lepc */
	/** @stable ICU 3.6 */
	USCRIPT_LINEAR_A UScriptCode = 83 /* Lina */
	/** @stable ICU 4.6 */
	USCRIPT_MANDAIC UScriptCode = 84 /* Mand */
	/** @stable ICU 3.6 */
	USCRIPT_MANDAEAN UScriptCode = USCRIPT_MANDAIC
	/** @stable ICU 3.6 */
	USCRIPT_MAYAN_HIEROGLYPHS UScriptCode = 85 /* Maya */
	/** @stable ICU 4.6 */
	USCRIPT_MEROITIC_HIEROGLYPHS UScriptCode = 86 /* Mero */
	/** @stable ICU 3.6 */
	USCRIPT_MEROITIC UScriptCode = USCRIPT_MEROITIC_HIEROGLYPHS
	/** @stable ICU 3.6 */
	USCRIPT_NKO UScriptCode = 87 /* Nkoo */
	/** @stable ICU 3.6 */
	USCRIPT_ORKHON UScriptCode = 88 /* Orkh */
	/** @stable ICU 3.6 */
	USCRIPT_OLD_PERMIC UScriptCode = 89 /* Perm */
	/** @stable ICU 3.6 */
	USCRIPT_PHAGS_PA UScriptCode = 90 /* Phag */
	/** @stable ICU 3.6 */
	USCRIPT_PHOENICIAN UScriptCode = 91 /* Phnx */
	/** @stable ICU 52 */
	USCRIPT_MIAO UScriptCode = 92 /* Plrd */
	/** @stable ICU 3.6 */
	USCRIPT_PHONETIC_POLLARD UScriptCode = USCRIPT_MIAO
	/** @stable ICU 3.6 */
	USCRIPT_RONGORONGO UScriptCode = 93 /* Roro */
	/** @stable ICU 3.6 */
	USCRIPT_SARATI UScriptCode = 94 /* Sara */
	/** @stable ICU 3.6 */
	USCRIPT_ESTRANGELO_SYRIAC UScriptCode = 95 /* Syre */
	/** @stable ICU 3.6 */
	USCRIPT_WESTERN_SYRIAC UScriptCode = 96 /* Syrj */
	/** @stable ICU 3.6 */
	USCRIPT_EASTERN_SYRIAC UScriptCode = 97 /* Syrn */
	/** @stable ICU 3.6 */
	USCRIPT_TENGWAR UScriptCode = 98 /* Teng */
	/** @stable ICU 3.6 */
	USCRIPT_VAI UScriptCode = 99 /* Vaii */
	/** @stable ICU 3.6 */
	USCRIPT_VISIBLE_SPEECH UScriptCode = 100 /* Visp */
	/** @stable ICU 3.6 */
	USCRIPT_CUNEIFORM UScriptCode = 101 /* Xsux */
	/** @stable ICU 3.6 */
	USCRIPT_UNWRITTEN_LANGUAGES UScriptCode = 102 /* Zxxx */
	/** @stable ICU 3.6 */
	USCRIPT_UNKNOWN UScriptCode = 103 /* Zzzz */ /* Unknown="Code for uncoded script", for unassigned code points */

	/** @stable ICU 3.8 */
	USCRIPT_CARIAN UScriptCode = 104 /* Cari */
	/** @stable ICU 3.8 */
	USCRIPT_JAPANESE UScriptCode = 105 /* Jpan */
	/** @stable ICU 3.8 */
	USCRIPT_LANNA UScriptCode = 106 /* Lana */
	/** @stable ICU 3.8 */
	USCRIPT_LYCIAN UScriptCode = 107 /* Lyci */
	/** @stable ICU 3.8 */
	USCRIPT_LYDIAN UScriptCode = 108 /* Lydi */
	/** @stable ICU 3.8 */
	USCRIPT_OL_CHIKI UScriptCode = 109 /* Olck */
	/** @stable ICU 3.8 */
	USCRIPT_REJANG UScriptCode = 110 /* Rjng */
	/** @stable ICU 3.8 */
	USCRIPT_SAURASHTRA UScriptCode = 111 /* Saur */
	/** Sutton SignWriting @stable ICU 3.8 */
	USCRIPT_SIGN_WRITING UScriptCode = 112 /* Sgnw */
	/** @stable ICU 3.8 */
	USCRIPT_SUNDANESE UScriptCode = 113 /* Sund */
	/** @stable ICU 3.8 */
	USCRIPT_MOON UScriptCode = 114 /* Moon */
	/** @stable ICU 3.8 */
	USCRIPT_MEITEI_MAYEK UScriptCode = 115 /* Mtei */

	/** @stable ICU 4.0 */
	USCRIPT_IMPERIAL_ARAMAIC UScriptCode = 116 /* Armi */
	/** @stable ICU 4.0 */
	USCRIPT_AVESTAN UScriptCode = 117 /* Avst */
	/** @stable ICU 4.0 */
	USCRIPT_CHAKMA UScriptCode = 118 /* Cakm */
	/** @stable ICU 4.0 */
	USCRIPT_KOREAN UScriptCode = 119 /* Kore */
	/** @stable ICU 4.0 */
	USCRIPT_KAITHI UScriptCode = 120 /* Kthi */
	/** @stable ICU 4.0 */
	USCRIPT_MANICHAEAN UScriptCode = 121 /* Mani */
	/** @stable ICU 4.0 */
	USCRIPT_INSCRIPTIONAL_PAHLAVI UScriptCode = 122 /* Phli */
	/** @stable ICU 4.0 */
	USCRIPT_PSALTER_PAHLAVI UScriptCode = 123 /* Phlp */
	/** @stable ICU 4.0 */
	USCRIPT_BOOK_PAHLAVI UScriptCode = 124 /* Phlv */
	/** @stable ICU 4.0 */
	USCRIPT_INSCRIPTIONAL_PARTHIAN UScriptCode = 125 /* Prti */
	/** @stable ICU 4.0 */
	USCRIPT_SAMARITAN UScriptCode = 126 /* Samr */
	/** @stable ICU 4.0 */
	USCRIPT_TAI_VIET UScriptCode = 127 /* Tavt */
	/** @stable ICU 4.0 */
	USCRIPT_MATHEMATICAL_NOTATION UScriptCode = 128 /* Zmth */
	/** @stable ICU 4.0 */
	USCRIPT_SYMBOLS UScriptCode = 129 /* Zsym */

	/** @stable ICU 4.4 */
	USCRIPT_BAMUM UScriptCode = 130 /* Bamu */
	/** @stable ICU 4.4 */
	USCRIPT_LISU UScriptCode = 131 /* Lisu */
	/** @stable ICU 4.4 */
	USCRIPT_NAKHI_GEBA UScriptCode = 132 /* Nkgb */
	/** @stable ICU 4.4 */
	USCRIPT_OLD_SOUTH_ARABIAN UScriptCode = 133 /* Sarb */

	/** @stable ICU 4.6 */
	USCRIPT_BASSA_VAH UScriptCode = 134 /* Bass */
	/** @stable ICU 54 */
	USCRIPT_DUPLOYAN UScriptCode = 135 /* Dupl */
	/** @stable ICU 4.6 */
	USCRIPT_ELBASAN UScriptCode = 136 /* Elba */
	/** @stable ICU 4.6 */
	USCRIPT_GRANTHA UScriptCode = 137 /* Gran */
	/** @stable ICU 4.6 */
	USCRIPT_KPELLE UScriptCode = 138 /* Kpel */
	/** @stable ICU 4.6 */
	USCRIPT_LOMA UScriptCode = 139 /* Loma */
	/** Mende Kikakui @stable ICU 4.6 */
	USCRIPT_MENDE UScriptCode = 140 /* Mend */
	/** @stable ICU 4.6 */
	USCRIPT_MEROITIC_CURSIVE UScriptCode = 141 /* Merc */
	/** @stable ICU 4.6 */
	USCRIPT_OLD_NORTH_ARABIAN UScriptCode = 142 /* Narb */
	/** @stable ICU 4.6 */
	USCRIPT_NABATAEAN UScriptCode = 143 /* Nbat */
	/** @stable ICU 4.6 */
	USCRIPT_PALMYRENE UScriptCode = 144 /* Palm */
	/** @stable ICU 54 */
	USCRIPT_KHUDAWADI UScriptCode = 145 /* Sind */
	/** @stable ICU 4.6 */
	USCRIPT_SINDHI UScriptCode = USCRIPT_KHUDAWADI
	/** @stable ICU 4.6 */
	USCRIPT_WARANG_CITI UScriptCode = 146 /* Wara */

	/** @stable ICU 4.8 */
	USCRIPT_AFAKA UScriptCode = 147 /* Afak */
	/** @stable ICU 4.8 */
	USCRIPT_JURCHEN UScriptCode = 148 /* Jurc */
	/** @stable ICU 4.8 */
	USCRIPT_MRO UScriptCode = 149 /* Mroo */
	/** @stable ICU 4.8 */
	USCRIPT_NUSHU UScriptCode = 150 /* Nshu */
	/** @stable ICU 4.8 */
	USCRIPT_SHARADA UScriptCode = 151 /* Shrd */
	/** @stable ICU 4.8 */
	USCRIPT_SORA_SOMPENG UScriptCode = 152 /* Sora */
	/** @stable ICU 4.8 */
	USCRIPT_TAKRI UScriptCode = 153 /* Takr */
	/** @stable ICU 4.8 */
	USCRIPT_TANGUT UScriptCode = 154 /* Tang */
	/** @stable ICU 4.8 */
	USCRIPT_WOLEAI UScriptCode = 155 /* Wole */

	/** @stable ICU 49 */
	USCRIPT_ANATOLIAN_HIEROGLYPHS UScriptCode = 156 /* Hluw */
	/** @stable ICU 49 */
	USCRIPT_KHOJKI UScriptCode = 157 /* Khoj */
	/** @stable ICU 49 */
	USCRIPT_TIRHUTA UScriptCode = 158 /* Tirh */

	/** @stable ICU 52 */
	USCRIPT_CAUCASIAN_ALBANIAN UScriptCode = 159 /* Aghb */
	/** @stable ICU 52 */
	USCRIPT_MAHAJANI UScriptCode = 160 /* Mahj */

	/** @stable ICU 54 */
	USCRIPT_AHOM UScriptCode = 161 /* Ahom */
	/** @stable ICU 54 */
	USCRIPT_HATRAN UScriptCode = 162 /* Hatr */
	/** @stable ICU 54 */
	USCRIPT_MODI UScriptCode = 163 /* Modi */
	/** @stable ICU 54 */
	USCRIPT_MULTANI UScriptCode = 164 /* Mult */
	/** @stable ICU 54 */
	USCRIPT_PAU_CIN_HAU UScriptCode = 165 /* Pauc */
	/** @stable ICU 54 */
	USCRIPT_SIDDHAM UScriptCode = 166 /* Sidd */

	/** @stable ICU 58 */
	USCRIPT_ADLAM UScriptCode = 167 /* Adlm */
	/** @stable ICU 58 */
	USCRIPT_BHAIKSUKI UScriptCode = 168 /* Bhks */
	/** @stable ICU 58 */
	USCRIPT_MARCHEN UScriptCode = 169 /* Marc */
	/** @stable ICU 58 */
	USCRIPT_NEWA UScriptCode = 170 /* Newa */
	/** @stable ICU 58 */
	USCRIPT_OSAGE UScriptCode = 171 /* Osge */

	/** @stable ICU 58 */
	USCRIPT_HAN_WITH_BOPOMOFO UScriptCode = 172 /* Hanb */
	/** @stable ICU 58 */
	USCRIPT_JAMO UScriptCode = 173 /* Jamo */
	/** @stable ICU 58 */
	USCRIPT_SYMBOLS_EMOJI UScriptCode = 174 /* Zsye */

	/** @stable ICU 60 */
	USCRIPT_MASARAM_GONDI UScriptCode = 175 /* Gonm */
	/** @stable ICU 60 */
	USCRIPT_SOYOMBO UScriptCode = 176 /* Soyo */
	/** @stable ICU 60 */
	USCRIPT_ZANABAZAR_SQUARE UScriptCode = 177 /* Zanb */

	/** @stable ICU 62 */
	USCRIPT_DOGRA UScriptCode = 178 /* Dogr */
	/** @stable ICU 62 */
	USCRIPT_GUNJALA_GONDI UScriptCode = 179 /* Gong */
	/** @stable ICU 62 */
	USCRIPT_MAKASAR UScriptCode = 180 /* Maka */
	/** @stable ICU 62 */
	USCRIPT_MEDEFAIDRIN UScriptCode = 181 /* Medf */
	/** @stable ICU 62 */
	USCRIPT_HANIFI_ROHINGYA UScriptCode = 182 /* Rohg */
	/** @stable ICU 62 */
	USCRIPT_SOGDIAN UScriptCode = 183 /* Sogd */
	/** @stable ICU 62 */
	USCRIPT_OLD_SOGDIAN UScriptCode = 184 /* Sogo */

	/** @stable ICU 64 */
	USCRIPT_ELYMAIC UScriptCode = 185 /* Elym */
	/** @stable ICU 64 */
	USCRIPT_NYIAKENG_PUACHUE_HMONG UScriptCode = 186 /* Hmnp */
	/** @stable ICU 64 */
	USCRIPT_NANDINAGARI UScriptCode = 187 /* Nand */
	/** @stable ICU 64 */
	USCRIPT_WANCHO UScriptCode = 188 /* Wcho */

	/** @stable ICU 66 */
	USCRIPT_CHORASMIAN UScriptCode = 189 /* Chrs */
	/** @stable ICU 66 */
	USCRIPT_DIVES_AKURU UScriptCode = 190 /* Diak */
	/** @stable ICU 66 */
	USCRIPT_KHITAN_SMALL_SCRIPT UScriptCode = 191 /* Kits */
	/** @stable ICU 66 */
	USCRIPT_YEZIDI UScriptCode = 192 /* Yezi */
)

func UScriptHasScript(c rune, sc UScriptCode) bool {
	scriptX := uchar.GetUnicodeProperties(c, 0) & UPROPS_SCRIPT_X_MASK
	codeOrIndex := mergeScriptCodeOrIndex(scriptX)
	if scriptX < UPROPS_SCRIPT_X_WITH_COMMON {
		return sc == UScriptCode(codeOrIndex)
	}

	scx := uchar.ScriptExtensions(codeOrIndex)
	if scriptX >= UPROPS_SCRIPT_X_WITH_OTHER {
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

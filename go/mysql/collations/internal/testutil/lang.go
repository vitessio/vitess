package testutil

import (
	"fmt"
	"regexp"
)

var KnownLanguages = map[Lang]string{
	"ja": "japanese",
	"vi": "vietnamese",
	"zh": "chinese",
	"ru": "russian",
	"hr": "croatian",
	"hu": "hungarian",
	"eo": "esperanto",
	"la": "latin",
	"es": "spanish",
	"sk": "slovak",
	"lt": "lithuanian",
	"da": "danish",
	"cs": "czech",
	"tr": "turkish",
	"sv": "swedish",
	"et": "estonian",
	"pl": "polish",
	"sl": "slovenian",
	"ro": "romanian",
	"lv": "latvian",
	"is": "icelandic",
	"de": "german",
	"fa": "persian",
	"en": "english",
	"si": "sinhala",
	"he": "hebrew",
	"rm": "roman",
	"el": "greek",
	"ko": "korean",
}

type Lang string

func (l Lang) Known() bool {
	_, valid := KnownLanguages[l]
	return valid
}

func (l Lang) Long() string {
	long, ok := KnownLanguages[l]
	if !ok {
		panic("unknown language")
	}
	return long
}

func (l Lang) MatchesCollation(collation string) bool {
	regex := fmt.Sprintf(`(\A|_)(%s\d?|%s_0900)_`, l.Long(), l)
	match, _ := regexp.MatchString(regex, collation)
	return match
}

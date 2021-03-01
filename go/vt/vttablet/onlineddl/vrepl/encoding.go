/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package vrepl

import (
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/simplifiedchinese"
)

type charsetEncoding map[string]encoding.Encoding

var charsetEncodingMap charsetEncoding

func init() {
	charsetEncodingMap = make(map[string]encoding.Encoding)
	// Begin mappings
	charsetEncodingMap["latin1"] = charmap.Windows1252
	charsetEncodingMap["gbk"] = simplifiedchinese.GBK
}

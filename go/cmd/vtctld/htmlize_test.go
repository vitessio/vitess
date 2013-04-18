package main

import (
	"testing"
)

type simpleStruct struct {
	Start, End int
}

type funkyStruct struct {
	SomeString  string
	SomeInt     int
	Embedded    simpleStruct
	EmptyString string
}

func TestHtmlize(t *testing.T) {
	o := funkyStruct{SomeString: "a string", SomeInt: 42, Embedded: simpleStruct{1, 42}}

	expected := `<dl class="funkyStruct"><dt>SomeString</dt><dd>a string</dd><dt>SomeInt</dt><dd>42</dd><dt>Embedded</dt><dd><dl class="simpleStruct"><dt>Start</dt><dd>1</dd><dt>End</dt><dd>42</dd></dl></dd><dt>EmptyString</dt><dd>&nbsp;</dd></dl>`
	if html := Htmlize(o); html != expected {
		t.Errorf("Wrong html: got %q, expected %q", html, expected)
	}
}

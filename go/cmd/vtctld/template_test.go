package main

import (
	"html/template"
	"testing"
)

func TestHtmlizeStruct(t *testing.T) {
	type simpleStruct struct {
		Start, End int
	}

	type funkyStruct struct {
		SomeString  string
		SomeInt     int
		Embedded    simpleStruct
		EmptyString string
	}
	input := funkyStruct{SomeString: "a string", SomeInt: 42, Embedded: simpleStruct{1, 42}}
	want := `<dl class="funkyStruct"><dt>SomeString</dt><dd>a string</dd><dt>SomeInt</dt><dd>42</dd><dt>Embedded</dt><dd><dl class="simpleStruct"><dt>Start</dt><dd>1</dd><dt>End</dt><dd>42</dd></dl></dd><dt>EmptyString</dt><dd>&nbsp;</dd></dl>`
	if got := Htmlize(input); got != want {
		t.Errorf("Htmlize: got %q, want %q", got, want)
	}
}

func TestHtmlizeMap(t *testing.T) {
	// We can't test multiple entries, since Htmlize supports maps whose keys can't be sorted.
	input := map[string]string{"dog": "apple"}
	want := `<dl class="map"><dt>dog</dt><dd>apple</dd></dl>`
	if got := Htmlize(input); got != want {
		t.Errorf("Htmlize: got %q, want %q", got, want)
	}
}

func TestHtmlizeSlice(t *testing.T) {
	input := []string{"aaa", "bbb", "ccc"}
	want := `<ul><li>aaa</li><li>bbb</li><li>ccc</li></ul>`
	if got := Htmlize(input); got != want {
		t.Errorf("Htmlize: got %q, want %q", got, want)
	}
}

func TestBreadCrumbs(t *testing.T) {
	input := "/grandparent/parent/node"
	want := template.HTML(`/<a href="/grandparent">grandparent</a>/<a href="/grandparent/parent">parent</a>/node`)
	if got := breadCrumbs(input); got != want {
		t.Errorf("breadCrumbs(%q) = %q, want %q", input, got, want)
	}
}

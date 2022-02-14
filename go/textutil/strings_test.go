package textutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitDelimitedList(t *testing.T) {
	defaultList := []string{"one", "two", "three"}
	tt := []struct {
		s    string
		list []string
	}{
		{s: "one,two,three"},
		{s: "one, two, three"},
		{s: "one,two; three  "},
		{s: "one two three"},
		{s: "one,,,two,three"},
		{s: " one, ,two,  three "},
	}

	for _, tc := range tt {
		if tc.list == nil {
			tc.list = defaultList
		}
		list := SplitDelimitedList(tc.s)
		assert.Equal(t, tc.list, list)
	}
}

func TestEscapeJoin(t *testing.T) {
	elems := []string{"normal", "with space", "with,comma", "with?question"}
	s := EscapeJoin(elems, ",")
	assert.Equal(t, "normal,with+space,with%2Ccomma,with%3Fquestion", s)
}

func TestSplitUnescape(t *testing.T) {
	{
		s := ""
		elems, err := SplitUnescape(s, ",")
		assert.NoError(t, err)
		assert.Nil(t, elems)
	}
	{
		s := "normal,with+space,with%2Ccomma,with%3Fquestion"
		expected := []string{"normal", "with space", "with,comma", "with?question"}
		elems, err := SplitUnescape(s, ",")
		assert.NoError(t, err)
		assert.Equal(t, expected, elems)
	}
}

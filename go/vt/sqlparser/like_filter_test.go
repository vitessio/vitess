package sqlparser

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEmptyLike(t *testing.T) {
	want := "^.*$"
	got := LikeToRegexp("").String()

	assert.Equal(t, want, got)
}

func TestLikePrefixRegexp(t *testing.T) {
	show, e := Parse("show vitess_metadata variables like 'key%'")
	if e != nil {
		t.Error(e)
	}

	want := "^key.*$"
	got := LikeToRegexp(show.(*Show).ShowTablesOpt.Filter.Like).String()

	assert.Equal(t, want, got)
}

func TestLikeAnyCharsRegexp(t *testing.T) {
	show, e := Parse("show vitess_metadata variables like '%val1%val2%'")
	if e != nil {
		t.Error(e)
	}

	want := "^.*val1.*val2.*$"
	got := LikeToRegexp(show.(*Show).ShowTablesOpt.Filter.Like).String()

	assert.Equal(t, want, got)
}

func TestSingleAndMultipleCharsRegexp(t *testing.T) {
	show, e := Parse("show vitess_metadata variables like '_val1_val2%'")
	if e != nil {
		t.Error(e)
	}

	want := "^.val1.val2.*$"
	got := LikeToRegexp(show.(*Show).ShowTablesOpt.Filter.Like).String()

	assert.Equal(t, want, got)
}

func TestSpecialCharactersRegexp(t *testing.T) {
	show, e := Parse("show vitess_metadata variables like '?.*?'")
	if e != nil {
		t.Error(e)
	}

	want := "^\\?\\.\\*\\?$"
	got := LikeToRegexp(show.(*Show).ShowTablesOpt.Filter.Like).String()

	assert.Equal(t, want, got)
}

func TestQuoteLikeSpecialCharacters(t *testing.T) {
	show, e := Parse(`show vitess_metadata variables like 'part1_part2\\%part3_part4\\_part5%'`)
	if e != nil {
		t.Error(e)
	}

	want := "^part1.part2%part3.part4_part5.*$"
	got := LikeToRegexp(show.(*Show).ShowTablesOpt.Filter.Like).String()

	assert.Equal(t, want, got)
}

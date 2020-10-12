/*
Copyright 2019 The Vitess Authors.

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

package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	got := LikeToRegexp(show.(*Show).Internal.(*ShowLegacy).ShowTablesOpt.Filter.Like).String()

	assert.Equal(t, want, got)
}

func TestLikeAnyCharsRegexp(t *testing.T) {
	show, e := Parse("show vitess_metadata variables like '%val1%val2%'")
	if e != nil {
		t.Error(e)
	}

	want := "^.*val1.*val2.*$"
	got := LikeToRegexp(show.(*Show).Internal.(*ShowLegacy).ShowTablesOpt.Filter.Like).String()

	assert.Equal(t, want, got)
}

func TestSingleAndMultipleCharsRegexp(t *testing.T) {
	show, e := Parse("show vitess_metadata variables like '_val1_val2%'")
	if e != nil {
		t.Error(e)
	}

	want := "^.val1.val2.*$"
	got := LikeToRegexp(show.(*Show).Internal.(*ShowLegacy).ShowTablesOpt.Filter.Like).String()

	assert.Equal(t, want, got)
}

func TestSpecialCharactersRegexp(t *testing.T) {
	show, e := Parse("show vitess_metadata variables like '?.*?'")
	if e != nil {
		t.Error(e)
	}

	want := "^\\?\\.\\*\\?$"
	got := LikeToRegexp(show.(*Show).Internal.(*ShowLegacy).ShowTablesOpt.Filter.Like).String()

	assert.Equal(t, want, got)
}

func TestQuoteLikeSpecialCharacters(t *testing.T) {
	show, e := Parse(`show vitess_metadata variables like 'part1_part2\\%part3_part4\\_part5%'`)
	if e != nil {
		t.Error(e)
	}

	want := "^part1.part2%part3.part4_part5.*$"
	got := LikeToRegexp(show.(*Show).Internal.(*ShowLegacy).ShowTablesOpt.Filter.Like).String()

	assert.Equal(t, want, got)
}

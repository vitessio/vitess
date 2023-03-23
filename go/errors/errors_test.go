/*
Copyright 2023 The Vitess Authors.

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

package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnwrap(t *testing.T) {
	err1 := errors.New("err1")
	err2 := errors.New("err2")
	err3 := errors.New("err3")
	err4 := errors.New("err4")

	tt := []struct {
		name              string
		err               error
		expectUnwrap      []error
		expectUnwrapAll   []error
		expectUnwrapFirst error
	}{
		{
			name:              "nil",
			expectUnwrap:      nil,
			expectUnwrapAll:   nil,
			expectUnwrapFirst: nil,
		},
		{
			name:              "single",
			err:               err1,
			expectUnwrap:      nil,
			expectUnwrapAll:   []error{err1},
			expectUnwrapFirst: err1,
		},
		{
			name:              "wrapped nil",
			err:               errors.Join(nil),
			expectUnwrap:      nil,
			expectUnwrapAll:   nil,
			expectUnwrapFirst: nil,
		},
		{
			name:              "single wrapped",
			err:               errors.Join(err1),
			expectUnwrap:      []error{err1},
			expectUnwrapAll:   []error{err1},
			expectUnwrapFirst: err1,
		},
		{
			name:              "flat wrapped",
			err:               errors.Join(err1, err2, err3, err4),
			expectUnwrap:      []error{err1, err2, err3, err4},
			expectUnwrapAll:   []error{err1, err2, err3, err4},
			expectUnwrapFirst: err1,
		},
		{
			name:              "double wrapped",
			err:               errors.Join(errors.Join(err1)),
			expectUnwrap:      []error{errors.Join(err1)},
			expectUnwrapAll:   []error{err1},
			expectUnwrapFirst: err1,
		},
		{
			name:              "double nested wrapped",
			err:               errors.Join(errors.Join(err1, err2), errors.Join(err3, err4)),
			expectUnwrap:      []error{errors.Join(err1, err2), errors.Join(err3, err4)},
			expectUnwrapAll:   []error{err1, err2, err3, err4},
			expectUnwrapFirst: err1,
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			unwrapped := Unwrap(tc.err)
			unwrappedAll := UnwrapAll(tc.err)
			unwrappedFirst := UnwrapFirst(tc.err)

			assert.Equal(t, tc.expectUnwrap, unwrapped)
			assert.Equal(t, tc.expectUnwrapAll, unwrappedAll)
			assert.Equal(t, tc.expectUnwrapFirst, unwrappedFirst)
		})
	}
}

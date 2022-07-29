/*
Copyright 2022 The Vitess Authors.

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

package schemadiff

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConstraintOriginalName(t *testing.T) {
	{
		names := []string{
			"check1",
			"check1_7no794p1x6zw6je1gfqmt7bca",
			"check1_8mp7joumtzb34562drvj9t7kh",
		}
		for _, name := range names {
			t.Run(name, func(t *testing.T) {
				originalName := ExtractConstraintOriginalName(name)
				assert.NotEmpty(t, originalName)
				assert.Equal(t, "check1", originalName)
			})
		}
	}
	{
		names := []string{
			"check1",
			"check_991ek3m5g69vcule23s9vnayd_check1",
			"fk_b9d15e218979970c23bfc13a_check1",
			"check1_bm701hx09ky10ygpj4imt6fbla",
			"check1_dbd0nmo0n05nl5fnckbdivkj",
		}
		for _, name := range names {
			t.Run(name, func(t *testing.T) {
				originalName := ExtractConstraintOriginalName(name)
				assert.NotEmpty(t, originalName)
				assert.Equal(t, name, originalName)
			})
		}
	}
}

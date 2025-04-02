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

func TestBlobStorageLength(t *testing.T) {
	assert.EqualValues(t, 1<<8-1, TinyBlogStorageLength)
	assert.EqualValues(t, 1<<16-1, BlobStorageLength)
	assert.EqualValues(t, 1<<24-1, MediumBlobStorageLength)
	assert.EqualValues(t, 1<<32-1, LongBlobStorageLength)
}

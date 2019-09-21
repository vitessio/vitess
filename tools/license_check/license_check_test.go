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
package main

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

var licenseHeader = `/*
LE LICENSE
*/
`

var wrongFile = `/*
SOMETHING VERY DIFFERENT
*/

so much code here...
`

var wrongFileWithBuildDirective = `// some line comment, and then a build directive
// +build race

/*
SOMETHING VERY DIFFERENT
*/

so much code here...
`

var rightFile = `/*
LE LICENSE
*/

so much code here...
`

var rightFileWithBuildDirectives = `// some line comment, and then a build directive
// +build race

/*
LE LICENSE
*/

so much code here...
`

func TestFileWithWrongLicenseHeader(t *testing.T) {
	setLicense(licenseHeader)

	assert.True(t, doesFileHaveCorrectHeader(s(rightFile)))
	assert.True(t, doesFileHaveCorrectHeader(s(rightFileWithBuildDirectives)))
	assert.False(t, doesFileHaveCorrectHeader(s(wrongFile)))

	assert.Equal(t, rightFile, replaceLicense(s(wrongFile), license))
	assert.Equal(t, rightFileWithBuildDirectives, replaceLicense(s(wrongFileWithBuildDirective), license))
}

func s(in string) []string {
	return strings.Split(in, "\n")
}
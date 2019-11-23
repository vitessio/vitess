// +build tools

/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tools

// These imports ensure that "go mod tidy" won't remove deps
// for build-time dependencies like linters and code generators
import (
	_ "github.com/golang/mock/mockgen"
	_ "golang.org/x/lint"
	_ "golang.org/x/tools/cmd/cover"
	_ "golang.org/x/tools/cmd/goimports"
	_ "golang.org/x/tools/cmd/goyacc"
	_ "honnef.co/go/tools/cmd/staticcheck"
)

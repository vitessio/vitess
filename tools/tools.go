// +build tools

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

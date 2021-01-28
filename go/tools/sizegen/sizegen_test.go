package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"golang.org/x/tools/go/packages"
)

func createFile(dir, fileName, code string) error {
	s := path.Join(dir, "a.go")
	return ioutil.WriteFile(s, []byte(code), os.ModePerm)
}

func TestName(t *testing.T) {
	dir, err := ioutil.TempDir("", "src")
	require.NoError(t, err)
	command := exec.Command("go", "mod", "init", "example.com/m")
	command.Dir = dir
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	err = command.Run()
	require.NoError(t, err)

	code := `
package code

type A struct {
}
`

	err = createFile(dir, "a.go", code)
	require.NoError(t, err)

	config := &packages.Config{
		Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesSizes | packages.NeedTypesInfo | packages.NeedDeps | packages.NeedImports | packages.NeedModule,
		Logf: log.Printf,
		Dir:  dir,
	}
	join := path.Join(dir, "...")
	initial, err := packages.Load(config, join)
	require.NoError(t, err)

	pkg := initial[0]
	require.Empty(t, pkg.Errors)
	assert.NotNil(t, pkg.Module)

	generator, err := generateCode(initial, []string{"example.com/m.A"})
	require.NoError(t, err)
	types := generator.generateRemainingKnownTypes()
	fmt.Println(types)
}

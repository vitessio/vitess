package codegen

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
)

type GoFile struct {
	name string
	buf  bytes.Buffer
}

func (g *GoFile) Write(p []byte) (n int, err error) {
	return g.buf.Write(p)
}

func (g *GoFile) Close() error {
	formatted, err := format.Source(g.buf.Bytes())
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad source:\n%s\n", g.buf.Bytes())
		return fmt.Errorf("failed to format generated code: %v", err)
	}

	err = os.WriteFile(g.name, formatted, 0644)
	if err != nil {
		return fmt.Errorf("failed to generate %q: %v", g.name, err)
	}
	return nil
}

func NewGoFile(path string) *GoFile {
	return &GoFile{name: path}
}

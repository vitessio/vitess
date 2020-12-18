package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

// ClientInterfaceDef is a struct providing enough information to generate an
// implementation of a gRPC Client interface.
type ClientInterfaceDef struct {
	PackageName string
	Type        string
	Imports     []*ImportDef
	Methods     []*MethodDef
}

// MethodDef is the variable part of a gRPC client interface method (i.e. not
// the context or dialopts arguments, or error return).
type MethodDef struct {
	Name         string
	RequestType  string
	ResponseType string
}

// ImportDef is the meta information about a Go import.
type ImportDef struct {
	Alias string
	Path  string
}

func parseInterfaceDefinition(b []byte) (*ClientInterfaceDef, error) {
	lines := bytes.Split(b, []byte("\n"))
	methods := make([]*MethodDef, 0, len(lines))

	for i, line := range lines {
		line := bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		parts := bytes.Split(line, []byte(" "))
		if len(parts) != 3 {
			return nil, fmt.Errorf("cannot parse interface definition; line %d did not have 3 parts", i+1)
		}

		methods = append(methods, &MethodDef{
			Name:         string(parts[0]),
			RequestType:  string(parts[1]),
			ResponseType: string(parts[2]),
		})
	}

	return &ClientInterfaceDef{
		Methods: methods,
	}, nil
}

func parseImportsList(b []byte) ([]*ImportDef, error) {
	lines := bytes.Split(b, []byte("\n"))
	imports := make([]*ImportDef, 0, len(lines))

	for i, line := range lines {
		line := bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		parts := bytes.Split(line, []byte(" "))
		switch len(parts) {
		case 1:
			imports = append(imports, &ImportDef{
				Path: string(parts[0]),
			})
		case 2:
			imports = append(imports, &ImportDef{
				Alias: string(parts[0]),
				Path:  string(parts[1]),
			})
		default:
			return nil, fmt.Errorf("cannot parse import definition; line %d did not have 1-2 parts", i+1)
		}
	}

	return imports, nil
}

func main() {
	pkgName := flag.String("package", "grpcvtctldclient", "package name")
	typeName := flag.String("type", "gRPCVtctldClient", "name of type to generate methods on")
	importsPath := flag.String("imports", "", "path to imports file")
	interfacePath := flag.String("interface", "", "path to interface file")
	out := flag.String("out", "", "output destination. empty to use stdout")

	flag.Parse()

	if *interfacePath == "" {
		panic("-interface is required")
	}

	var output io.Writer = os.Stdout

	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			panic(err)
		}

		defer f.Close()
		output = f
	}

	interfaceDef, err := ioutil.ReadFile(*interfacePath)
	if err != nil {
		panic(err)
	}

	iface, err := parseInterfaceDefinition(interfaceDef)
	if err != nil {
		panic(err)
	}

	if *importsPath != "" {
		importsList, err := ioutil.ReadFile(*importsPath)
		if err != nil {
			panic(err)
		}

		iface.Imports, err = parseImportsList(importsList)
		if err != nil {
			panic(err)
		}
	}

	iface.PackageName = *pkgName
	iface.Type = *typeName

	if err := tmpl.Execute(output, iface); err != nil {
		panic(err)
	}
}

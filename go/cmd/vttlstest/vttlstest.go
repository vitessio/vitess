/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"io"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/tlstest"

	// Include deprecation warnings for soon-to-be-unsupported flag invocations.
	_flag "vitess.io/vitess/go/internal/flag"
)

var doc = `
vttlstest is a tool for generating test certificates and keys for TLS tests.

To create a toplevel CA, use:
  $ vttlstest --root /tmp CreateCA

To create an intermediate CA, use:
  $ vttlstest --root /tmp CreateIntermediateCA servers

To create a leaf certificate, use:
  $ vttlstest --root /tmp CreateSignedCert --parent servers server

To get help on a command, use:
  $ vttlstest <command> --help
`

type cmdFunc func(subFlags *flag.FlagSet, args []string)

var cmdMap map[string]cmdFunc

func init() {
	cmdMap = map[string]cmdFunc{
		"CreateCA":             cmdCreateCA,
		"CreateCRL":            cmdCreateCRL,
		"CreateIntermediateCA": cmdCreateIntermediateCA,
		"CreateSignedCert":     cmdCreateSignedCert,
		"RevokeCert":           cmdRevokeCert,
	}
}

var (
	root = flag.String("root", ".", "root directory for certificates and keys")
)

func cmdCreateCA(subFlags *flag.FlagSet, args []string) {
	subFlags.Parse(args)
	if subFlags.NArg() > 0 {
		log.Fatalf("CreateCA command doesn't take any parameter")
	}

	tlstest.CreateCA(*root)
}

func cmdCreateCRL(subFlags *flag.FlagSet, args []string) {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		log.Fatalf("CreateCRL command takes a single CA name as a parameter")
	}

	ca := subFlags.Arg(0)
	tlstest.CreateCRL(*root, ca)
}

func cmdRevokeCert(subFlags *flag.FlagSet, args []string) {
	parent := subFlags.String("parent", "ca", "Parent cert name to use. Use 'ca' for the toplevel CA.")

	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		log.Fatalf("RevokeCert command takes a single name as a parameter")
	}

	name := subFlags.Arg(0)
	tlstest.RevokeCertAndRegenerateCRL(*root, *parent, name)
}

func cmdCreateIntermediateCA(subFlags *flag.FlagSet, args []string) {
	parent := subFlags.String("parent", "ca", "Parent cert name to use. Use 'ca' for the toplevel CA.")
	serial := subFlags.String("serial", "01", "Serial number for the certificate to create. Should be different for two certificates with the same parent.")
	commonName := subFlags.String("common_name", "", "Common name for the certificate. If empty, uses the name.")

	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		log.Fatalf("CreateIntermediateCA command takes a single name as a parameter")
	}

	name := subFlags.Arg(0)
	if *commonName == "" {
		*commonName = name
	}

	tlstest.CreateIntermediateCA(*root, *parent, *serial, name, *commonName)
}

func cmdCreateSignedCert(subFlags *flag.FlagSet, args []string) {
	parent := subFlags.String("parent", "ca", "Parent cert name to use. Use 'ca' for the toplevel CA.")
	serial := subFlags.String("serial", "01", "Serial number for the certificate to create. Should be different for two certificates with the same parent.")
	commonName := subFlags.String("common_name", "", "Common name for the certificate. If empty, uses the name.")

	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		log.Fatalf("CreateSignedCert command takes a single name as a parameter")
	}

	name := subFlags.Arg(0)
	if *commonName == "" {
		*commonName = name
	}

	tlstest.CreateSignedCert(*root, *parent, *serial, name, *commonName)
}

func main() {
	defer exit.Recover()
	defer logutil.Flush()
	_flag.SetUsage(flag.CommandLine, _flag.UsageOptions{
		Preface: func(w io.Writer) { fmt.Fprint(w, doc) },
	})
	_flag.Parse(pflag.NewFlagSet("vttlstest", pflag.ExitOnError))
	args := _flag.Args()
	if len(args) == 0 {
		flag.Usage()
		exit.Return(1)
	}

	cmdName := args[0]
	args = args[1:]
	cmd, ok := cmdMap[cmdName]
	if !ok {
		log.Fatalf("Unknown command %v", cmdName)
	}
	subFlags := flag.NewFlagSet(cmdName, flag.ExitOnError)

	// Run the command.
	cmd(subFlags, args)
}

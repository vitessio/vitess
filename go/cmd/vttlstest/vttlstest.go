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
	"fmt"
	"os"

	flag "github.com/spf13/pflag"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/tlstest"
)

type cmdFunc func(subFlags *flag.FlagSet, args []string)

const doc = `
 vttlstest is a tool for generating test certificates and keys for TLS tests.

To create a toplevel CA, use:
  $ vttlstest [--root <dir>] CreateCA
To create an intermediate CA, use:
  $ vttlstest [--root <dir>] [--parent <name>] [--serial <serial num>] [--common-name <name>] CreateIntermediateCA <name>
To create a certficate revocation list, use:
  $ vttlstest [--root <dir>] CreateCRL <server>
To create a leaf certificate, use:
  $ vttlstest [--root <dir>] [--parent <parent CA name>] [--serial <serial num>] [--common-name <name>] CreateSignedCert <cert name>
To revoke a certificate, use:
  $ vttlstest [--root <directory>] [--parent <name>] RevokeCert <name>
`

var (
	cmdMap map[string]cmdFunc
	root   = "."
)

func init() {
	cmdMap = map[string]cmdFunc{
		"CreateCA":             cmdCreateCA,
		"CreateIntermediateCA": cmdCreateIntermediateCA,
		"CreateCRL":            cmdCreateCRL,
		"CreateSignedCert":     cmdCreateSignedCert,
		"RevokeCert":           cmdRevokeCert,
	}

	servenv.OnParse(func(fs *flag.FlagSet) {
		fs.StringVar(&root, "root", root, "root directory for certificates and keys")
	})
}

func cmdCreateCA(subFlags *flag.FlagSet, args []string) {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: vttlstest [--root <directory>] CreateCA\n")
		os.Exit(1)
	}
	_ = subFlags.Parse(args)
	if subFlags.NArg() > 0 {
		flag.Usage()
	}

	tlstest.CreateCA(root)
}

func cmdCreateIntermediateCA(subFlags *flag.FlagSet, args []string) {
	parent := subFlags.String("parent", "ca", "Parent cert name to use. Use 'ca' for the toplevel CA.")
	serial := subFlags.String("serial", "01", "Serial number for the certificate to create. Should be different for two certificates with the same parent.")
	commonName := subFlags.String("common-name", "", "Common name for the certificate. If empty, uses the name.")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: vttlstest [--root <dir>] [--parent <parent CA name>] [--serial <serial num>] [--common-name <name>] CreateIntermediateCA <CA name>\n")
		os.Exit(1)
	}
	_ = subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		flag.Usage()
	}

	name := subFlags.Arg(0)
	if *commonName == "" {
		*commonName = name
	}

	tlstest.CreateIntermediateCA(root, *parent, *serial, name, *commonName)
}

func cmdCreateCRL(subFlags *flag.FlagSet, args []string) {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: vttlstest [--root <directory>] CreateCRL <CA name>\n")
		os.Exit(1)
	}
	_ = subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		flag.Usage()
	}

	ca := subFlags.Arg(0)
	tlstest.CreateCRL(root, ca)
}

func cmdCreateSignedCert(subFlags *flag.FlagSet, args []string) {
	parent := subFlags.String("parent", "ca", "Parent cert name to use. Use 'ca' for the toplevel CA.")
	serial := subFlags.String("serial", "01", "Serial number for the certificate to create. Should be different for two certificates with the same parent.")
	commonName := subFlags.String("common-name", "", "Common name for the certificate. If empty, uses the name.")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: vttlstest [--root <dir>] [--parent <parent CA name>] [--serial <serial num>] [--common-name <name>] CreateSignedCert <cert name>\n")
		os.Exit(1)
	}
	_ = subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		flag.Usage()
	}

	name := subFlags.Arg(0)
	if *commonName == "" {
		*commonName = name
	}

	tlstest.CreateSignedCert(root, *parent, *serial, name, *commonName)
}

func cmdRevokeCert(subFlags *flag.FlagSet, args []string) {
	parent := subFlags.String("parent", "ca", "Parent cert name to use. Use 'ca' for the toplevel CA.")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: vttlstest [--root <directory>] [--parent <parent cert name>] RevokeCert <cert name>\n")
		os.Exit(1)
	}
	_ = subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		flag.Usage()
	}

	name := subFlags.Arg(0)
	tlstest.RevokeCertAndRegenerateCRL(root, *parent, name)
}

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprint(os.Stderr, doc)
		fmt.Fprintln(os.Stderr)
		flag.PrintDefaults()
		os.Exit(1)
	}

	if len(os.Args) == 1 {
		flag.Usage()
	}

	args := servenv.ParseFlagsWithArgs("vttlstest")
	cmdName := args[0]
	args = args[1:]
	cmd, ok := cmdMap[cmdName]
	if !ok {
		fmt.Fprintf(os.Stderr, "Unknown command: %v\n\n", cmdName)
		flag.Usage()
	}
	subFlags := flag.NewFlagSet(cmdName, flag.ContinueOnError)

	// Run the command.
	cmd(subFlags, args)
}

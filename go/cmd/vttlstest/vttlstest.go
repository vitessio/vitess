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
	"flag"
	"fmt"
	"os"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/tlstest"
)

var doc = `
vttlstest is a tool for generating test certificates and keys for TLS tests.

To create a toplevel CA, use:
  $ vttlstest -root /tmp CreateCA

To create an intermediate or leaf CA, use:
  $ vttlstest -root /tmp CreateSignedCert servers
  $ vttlstest -root /tmp CreateSignedCert -parent servers server

To get help on a command, use:
  $ vttlstest <command> -help
`

type cmdFunc func(subFlags *flag.FlagSet, args []string)

var cmdMap map[string]cmdFunc

func init() {
	cmdMap = map[string]cmdFunc{
		"CreateCA":         cmdCreateCA,
		"CreateSignedCert": cmdCreateSignedCert,
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

func cmdCreateSignedCert(subFlags *flag.FlagSet, args []string) {
	parent := subFlags.String("parent", "ca", "Parent cert name to use. Use 'ca' for the toplevel CA.")
	serial := subFlags.String("serial", "01", "Serial number for the certificate to create. Should be different for two certificates with the same parent.")
	commonName := subFlags.String("common_name", "", "Common name for the certificate. If empty, uses the name.")

	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		log.Fatalf("CreateSignedCert command takes a single name as a parameter")
	}
	if *commonName == "" {
		*commonName = subFlags.Arg(0)
	}

	tlstest.CreateSignedCert(*root, *parent, *serial, subFlags.Arg(0), *commonName)
}

func main() {
	defer exit.Recover()
	defer logutil.Flush()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %v:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprint(os.Stderr, doc)
	}
	flag.Parse()
	args := flag.Args()
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

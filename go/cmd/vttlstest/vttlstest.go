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
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/tlstest"
)

var (
	root       = "."
	parent     = "ca"
	serial     = "01"
	commonName string

	rootCmd = &cobra.Command{
		Use:   "vttlstest",
		Short: "vttlstest is a tool for generating test certificates, keys, and related artifacts for TLS tests.",
		Long:  "vttlstest is a tool for generating test certificates, keys, and related artifacts for TLS tests.",
	}

	createCACmd = &cobra.Command{
		Use:                   "CreateCA [--root <dir>]",
		DisableFlagsInUseLine: true,
		Example:               "CreateCA --root /tmp",
		Short:                 "Create certificate authority",
		Long:                  "Create certificate authority",
		Args:                  cobra.NoArgs,
		Run:                   runCreateCA,
	}

	createIntermediateCACmd = &cobra.Command{
		Use:                   "CreateIntermediateCA [--root <dir>] [--parent <name>] [--serial <serial>] [--common-name <CN>] <CA name>",
		DisableFlagsInUseLine: true,
		Example:               "CreateIntermediateCA --root /tmp --parent ca mail.mycoolsite.com",
		Short:                 "Create intermediate certificate authority",
		Long:                  "Create intermediate certificate authority",
		Args:                  cobra.ExactArgs(1),
		Run:                   runCreateIntermediateCA,
	}

	createCRLCmd = &cobra.Command{
		Use:                   "CreateCRL [--root <dir>] <server>",
		DisableFlagsInUseLine: true,
		Example:               "CreateCRL --root /tmp mail.mycoolsite.com",
		Short:                 "Create certificate revocation list",
		Long:                  "Create certificate revocation list",
		Args:                  cobra.ExactArgs(1),
		Run:                   runCreateCRL,
	}

	createSignedCertCmd = &cobra.Command{
		Use:                   "CreateSignedCert [--root <dir>] [--parent <name>] [--serial <serial>] [--common-name <CN>] <cert name>",
		DisableFlagsInUseLine: true,
		Example:               "CreateSignedCert --root /tmp --common-name mail.mysite.com --parent mail.mycoolsite.com postman1",
		Short:                 "Create signed certificate",
		Long:                  "Create signed certificate",
		Args:                  cobra.ExactArgs(1),
		Run:                   runCreateSignedCert,
	}

	revokeCertCmd = &cobra.Command{
		Use:                   "RevokeCert [--root <dir>] [--parent <name>] <cert name>",
		DisableFlagsInUseLine: true,
		Example:               "RevokeCert --root /tmp --parent mail.mycoolsite.com postman1",
		Short:                 "Revoke a certificate",
		Long:                  "Revoke a certificate",
		Args:                  cobra.ExactArgs(1),
		Run:                   runRevokeCert,
	}
)

func init() {
	rootCmd.PersistentFlags().StringVar(&root, "root", root, "root directory for all artifacts")

	rootCmd.AddCommand(createCACmd)
	rootCmd.AddCommand(createIntermediateCACmd)
	rootCmd.AddCommand(createCRLCmd)
	rootCmd.AddCommand(createSignedCertCmd)
	rootCmd.AddCommand(revokeCertCmd)

	for _, cmd := range []*cobra.Command{createIntermediateCACmd, createSignedCertCmd} {
		cmd.Flags().StringVar(&parent, "parent", parent, "Parent cert name to use. Use 'ca' for the toplevel CA.")
		cmd.Flags().StringVar(&serial, "serial", serial, "Serial number for the certificate to create. Should be different for two certificates with the same parent.")
		cmd.Flags().StringVar(&commonName, "common-name", commonName, "Common name for the certificate. If empty, uses the name.")
	}
	revokeCertCmd.Flags().StringVar(&parent, "parent", parent, "Parent cert name to use. Use 'ca' for the toplevel CA.")
}

func runCreateCA(cmd *cobra.Command, args []string) {
	tlstest.CreateCA(root)
}

func runCreateIntermediateCA(cmd *cobra.Command, args []string) {
	name := args[0]
	if commonName == "" {
		commonName = name
	}

	tlstest.CreateIntermediateCA(root, parent, serial, name, commonName)
}

func runCreateCRL(cmd *cobra.Command, args []string) {
	ca := args[0]
	tlstest.CreateCRL(root, ca)
}

func runCreateSignedCert(cmd *cobra.Command, args []string) {
	name := args[0]
	if commonName == "" {
		commonName = name
	}

	tlstest.CreateSignedCert(root, parent, serial, name, commonName)
}

func runRevokeCert(cmd *cobra.Command, args []string) {
	name := args[0]
	tlstest.RevokeCertAndRegenerateCRL(root, parent, name)
}

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	cobra.CheckErr(rootCmd.Execute())
}

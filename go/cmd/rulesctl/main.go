package main

import (
	"log"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cmd/rulesctl/cmd"
	vtlog "vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
)

func main() {
	rootCmd := cmd.Main()
	rootCmd.SetGlobalNormalizationFunc(utils.NormalizeUnderscoresToDashes)
	vtlog.RegisterFlags(rootCmd.PersistentFlags())
	logutil.RegisterFlags(rootCmd.PersistentFlags())
	acl.RegisterFlags(rootCmd.PersistentFlags())
	servenv.RegisterMySQLServerFlags(rootCmd.PersistentFlags())
	if err := rootCmd.Execute(); err != nil {
		log.Printf("%v", err)
	}
}

// Package vtaclcheck analyzes a set of sql statements and returns the
// corresponding vtgate and vttablet query plans that will be executed
// on the given statements
package vtaclcheck

import (
	"fmt"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"
)

// Options to control the explain process
type Options struct {
	// AclFile is the file with the JSON acl configuration
	ACLFile string
	// StaticAuthFile is the file containing the mysql auth_server_static JSON
	StaticAuthFile string
}

var options *Options

// Init sets up the fake execution environment
func Init(opts *Options) error {
	// verify opts is defined
	if opts == nil {
		return fmt.Errorf("vtaclcheck.Init: opts is NULL")
	}
	// Verify options
	if opts.ACLFile == "" && opts.StaticAuthFile == "" {
		return fmt.Errorf("no options specified")
	}

	options = opts

	return nil
}

// Run the check on the given file
func Run() error {
	if options.ACLFile != "" {
		tableacl.Register("simpleacl", &simpleacl.Factory{})
		err := tableacl.Init(
			options.ACLFile,
			func() {},
		)
		if err != nil {
			return fmt.Errorf("fail to initialize Table ACL: %v", err)
		}

		fmt.Printf("JSON ACL file %s looks good\n", options.ACLFile)
	}

	if options.StaticAuthFile != "" {
		mysql.RegisterAuthServerStaticFromParams(options.StaticAuthFile, "", 0)

		fmt.Printf("Static auth file %s looks good\n", options.StaticAuthFile)
	}

	return nil
}

// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"flag"
	"fmt"

	log "github.com/golang/glog"
)

// This file handles using command line flags to create a Mycnf object.
// Since whoever links with this module doesn't necessarely need the flags,
// RegisterFlags needs to be called explicitely to set the flags up.

var (
	flagServerId  *int
	flagMycnfFile *string
)

// RegisterFlags registers the command line flags for
// specifying the values of a mycnf config file. See NewMycnfFromFlags
// to get the supported modes.
func RegisterFlags() {
	flagServerId = flag.Int("mycnf_server_id", 0, "mysql server id of the server (if specified, mycnf-file will be ignored)")
	flagMycnfFile = flag.String("mycnf-file", "", "path to my.cnf, if reading all config params from there")
}

// NewMycnfFromFlags creates a Mycnf object from the command line flags.
//
// Multiple modes are supported:
// - at least mycnf_server_id is set on the command line
//   --> then we read all parameters from the command line, and not from
//       any my.cnf file.
// - mycnf_server_id is not passed in, but mycnf-file is passed in
//   --> then we read that mycnf file
// - mycnf_server_id and mycnf-file are not passed in:
//   --> then we use the default location of the my.cnf file for the
//       provided uid and read that my.cnf file.
//
// RegisterCommandLineFlags should have been called before calling
// this, otherwise we'll panic.
func NewMycnfFromFlags(uid uint32) (mycnf *Mycnf, err error) {
	if *flagServerId != 0 {
		log.Info("mycnf_server_id is specified, using command line parameters for mysql config")
		panic("NYI")
		return &Mycnf{}, nil
	} else {
		if *flagMycnfFile == "" {
			if uid == 0 {
				log.Fatalf("No mycnf_server_id, no mycnf-file, and no backup server id to use")
			}
			*flagMycnfFile = mycnfFile(uid)
			log.Infof("No mycnf_server_id, no mycnf-file specified, using default config for server id %v: %v", uid, *flagMycnfFile)
		} else {
			log.Infof("No mycnf_server_id specified, using mycnf-file file %v", *flagMycnfFile)
		}
		return ReadMycnf(*flagMycnfFile)
	}
}

// GetSubprocessFlags returns the flags to pass to a subprocess to
// have the exact same mycnf config as us.
//
// RegisterCommandLineFlags and NewMycnfFromFlags should have been
// called before this.
func GetSubprocessFlags() []string {
	if *flagServerId != 0 {
		// all from command line
		return []string{
			"-mycnf_server_id", fmt.Sprintf("%v", *flagServerId),
		}
	}

	// Just pass through the mycnf-file param, it has been altered
	// if we didn't get it but guessed it from uid.
	return []string{"-mycnf-file", *flagMycnfFile}
}

package common

import (
	"log"

	"golang.org/x/tools/go/packages"
)

// PkgFailed returns true if any of the packages contain errors
func PkgFailed(loaded []*packages.Package) bool {
	failed := false
	for _, pkg := range loaded {
		for _, e := range pkg.Errors {
			log.Println(e.Error())
			failed = true
		}
	}
	return failed
}

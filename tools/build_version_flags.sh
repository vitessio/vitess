#!/bin/bash

# goversion_min returns true if major.minor go version is at least some value.
function goversion_min() {
	[[ "$(go version)" =~ go([0-9]+)\.([0-9]+) ]]
	gotmajor=${BASH_REMATCH[1]}
	gotminor=${BASH_REMATCH[2]}
	[[ "$1" =~ ([0-9]+)\.([0-9]+) ]]
	wantmajor=${BASH_REMATCH[1]}
	wantminor=${BASH_REMATCH[2]}
	[ "$gotmajor" -lt "$wantmajor" ] && return 1
	[ "$gotmajor" -gt "$wantmajor" ] && return 0
	[ "$gotminor" -lt "$wantminor" ] && return 1
	return 0
}

# Starting with Go 1.5, the syntax for the -X flag changed.
# Earlier Go versions don't support the new syntax.
if goversion_min 1.5; then
	echo "\
	-X 'github.com/youtube/vitess/go/vt/servenv.buildHost=$(hostname)'\
	-X 'github.com/youtube/vitess/go/vt/servenv.buildUser=$(whoami)'\
	-X 'github.com/youtube/vitess/go/vt/servenv.buildGitRev=$(git rev-parse HEAD)'\
	-X 'github.com/youtube/vitess/go/vt/servenv.buildTime=$(LC_ALL=C date)'\
	"
else
	echo "\
	-X github.com/youtube/vitess/go/vt/servenv.buildHost   '$(hostname)'\
	-X github.com/youtube/vitess/go/vt/servenv.buildUser   '$(whoami)'\
	-X github.com/youtube/vitess/go/vt/servenv.buildGitRev '$(git rev-parse HEAD)'\
	-X github.com/youtube/vitess/go/vt/servenv.buildTime   '$(LC_ALL=C date)'\
	"
fi

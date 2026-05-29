//go:build linux

/*
Copyright 2026 The Vitess Authors.

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

// fuse_helper is a minimal loopback FUSE filesystem used by the
// disk_health_monitor end-to-end test. It mirrors -backing onto -mount and,
// once the mount is live, prints "READY" on a line by itself. The test
// stalls the filesystem by sending SIGSTOP to this process; SIGCONT
// resumes it. SIGTERM/SIGINT unmounts cleanly.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func main() {
	mountPoint := flag.String("mount", "", "directory to mount the FUSE filesystem on")
	backing := flag.String("backing", "", "directory whose contents are mirrored through the mount")
	flag.Parse()

	if *mountPoint == "" || *backing == "" {
		fmt.Fprintln(os.Stderr, "both -mount and -backing are required")
		os.Exit(2)
	}

	loopbackRoot, err := fs.NewLoopbackRoot(*backing)
	if err != nil {
		log.Fatalf("NewLoopbackRoot(%s): %v", *backing, err)
	}

	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: *backing,
			Name:   "vitess-disk-health-monitor-test",
		},
	}

	server, err := fs.Mount(*mountPoint, loopbackRoot, opts)
	if err != nil {
		log.Fatalf("fs.Mount(%s): %v", *mountPoint, err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		_ = server.Unmount()
	}()

	fmt.Println("READY")
	server.Wait()
}

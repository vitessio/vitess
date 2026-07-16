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

// fuse_helper is a passthrough loopback FUSE filesystem used by the
// disk_health_monitor end-to-end tests. It mirrors -backing onto -mount
// and supports a small set of catchable signals that toggle simulated
// disk-failure modes for VTTablet to react to.
//
// Signal protocol (must stay in sync with the test):
//
//	SIGUSR1 — enter "stalled" mode. Mutating FUSE ops block until cleared.
//	SIGUSR2 — RESERVED for "disk full" mode (writes will return ENOSPC).
//	            Wired in a follow-up PR; SIGUSR2 is intentionally claimed
//	            now so the protocol is stable.
//	SIGHUP  — clear any failure mode; resume normal operation.
//	SIGTERM / SIGINT — unmount cleanly and exit.
//
// We deliberately do NOT use SIGSTOP/SIGCONT: while they would stall the
// kernel-side FUSE daemon, they also freeze this process's Go runtime, so
// a SIGTERM during a stall cannot be delivered until SIGCONT is sent. A
// test panic mid-stall would then wedge cluster teardown on a hung FUSE
// mount. Catchable signals + in-process gating keep the helper responsive
// to SIGTERM at all times.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// gate blocks callers of wait() while in stall mode. Mode transitions are
// driven by signals; see the signal protocol at the top of the file.
type gate struct {
	mu      sync.Mutex
	blockCh chan struct{} // non-nil and open when stalled — receivers block
}

// wait blocks if the gate is currently in stall mode, returning as soon as
// resume() closes the block channel. When not stalled it returns immediately.
func (g *gate) wait() {
	g.mu.Lock()
	ch := g.blockCh
	g.mu.Unlock()
	if ch != nil {
		<-ch
	}
}

func (g *gate) stall() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.blockCh == nil {
		g.blockCh = make(chan struct{})
	}
}

func (g *gate) resume() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.blockCh != nil {
		close(g.blockCh) // unblocks every pending waiter
		g.blockCh = nil
	}
}

var ioGate = &gate{}

// gatedNode wraps fs.LoopbackNode and routes mutating operations through
// ioGate.wait(). Together with gatedFile (returned from Create/Open) this
// covers the full surface vttablet's disk health monitor probe and mysqld
// exercise during a stall:
//
//	Node-level (gatedNode):   Create, Open, Setattr
//	Handle-level (gatedFile): Write, Fsync
//
// Read-only operations and metadata ops outside this set remain pass-through.
type gatedNode struct {
	fs.LoopbackNode
}

func newGatedNode(rootData *fs.LoopbackRoot, _ *fs.Inode, _ string, _ *syscall.Stat_t) fs.InodeEmbedder {
	return &gatedNode{LoopbackNode: fs.LoopbackNode{RootData: rootData}}
}

func (n *gatedNode) Create(ctx context.Context, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	ioGate.wait()
	inode, fh, fuseFlags, errno := n.LoopbackNode.Create(ctx, name, flags, mode, out)
	return inode, wrapFileHandle(fh), fuseFlags, errno
}

func (n *gatedNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	ioGate.wait()
	fh, fuseFlags, errno := n.LoopbackNode.Open(ctx, flags)
	return wrapFileHandle(fh), fuseFlags, errno
}

func (n *gatedNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	ioGate.wait()
	// Setattr can be called with one of our wrapped handles; unwrap so
	// LoopbackNode sees the *LoopbackFile it expects.
	if gf, ok := f.(*gatedFile); ok {
		f = gf.LoopbackFile
	}
	return n.LoopbackNode.Setattr(ctx, f, in, out)
}

// gatedFile wraps *fs.LoopbackFile and gates the mutating operations issued
// against an already-open handle. Read/Lseek/Flush/Release/etc. are promoted
// via embedding and pass through unchanged, so they remain available for
// go-fuse's interface type assertions.
type gatedFile struct {
	*fs.LoopbackFile
}

// wrapFileHandle wraps a LoopbackFile in a gatedFile so its mutating
// operations are gated. Non-LoopbackFile handles pass through unchanged —
// shouldn't happen with a stock LoopbackRoot, but keeps the helper robust if
// go-fuse ever returns a different concrete type.
func wrapFileHandle(fh fs.FileHandle) fs.FileHandle {
	if fh == nil {
		return nil
	}
	if lf, ok := fh.(*fs.LoopbackFile); ok {
		return &gatedFile{LoopbackFile: lf}
	}
	return fh
}

func (f *gatedFile) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	ioGate.wait()
	return f.LoopbackFile.Write(ctx, data, off)
}

func (f *gatedFile) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	ioGate.wait()
	return f.LoopbackFile.Fsync(ctx, flags)
}

func main() {
	mountPoint := flag.String("mount", "", "directory to mount the FUSE filesystem on")
	backing := flag.String("backing", "", "directory whose contents are mirrored through the mount")
	flag.Parse()

	if *mountPoint == "" || *backing == "" {
		fmt.Fprintln(os.Stderr, "both -mount and -backing are required")
		os.Exit(2)
	}

	var st syscall.Stat_t
	if err := syscall.Stat(*backing, &st); err != nil {
		log.Fatalf("stat(%s): %v", *backing, err)
	}

	root := &fs.LoopbackRoot{
		Path:    *backing,
		Dev:     uint64(st.Dev),
		NewNode: newGatedNode,
	}
	rootNode := newGatedNode(root, nil, "", &st)
	root.RootNode = rootNode

	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: *backing,
			Name:   "vitess-disk-health-monitor-test",
		},
	}

	server, err := fs.Mount(*mountPoint, rootNode, opts)
	if err != nil {
		log.Fatalf("fs.Mount(%s): %v", *mountPoint, err)
	}

	// Failure-mode signals (USR1/USR2/HUP) — see signal protocol at top of file.
	modeSig := make(chan os.Signal, 4)
	signal.Notify(modeSig, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGHUP)
	go func() {
		for s := range modeSig {
			switch s {
			case syscall.SIGUSR1:
				// simulate a stalled filesystem/volume
				ioGate.stall()
			case syscall.SIGHUP:
				// un-stall the filesystem/volume
				ioGate.resume()
			case syscall.SIGUSR2:
				// Reserved for "disk full" mode; wired in a follow-up PR.
				// Until then, treat as a no-op so a stray SIGUSR2 doesn't
				// look like a stall or kill the process.
			}
		}
	}()

	// Teardown signals.
	termSig := make(chan os.Signal, 1)
	signal.Notify(termSig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-termSig
		// Resume first so any waiters drain and in-flight ops can complete
		// before we tear down the mount — otherwise Unmount can return EBUSY.
		ioGate.resume()
		_ = server.Unmount()
	}()

	fmt.Println("READY")
	server.Wait()
}

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

package testfs

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type gateMode int

const (
	modeHealthy gateMode = iota
	modeStalled
	modeFull
)

const (
	signalStalled = syscall.SIGUSR1
	signalFull    = syscall.SIGUSR2
	signalClear   = syscall.SIGHUP
)

type gate struct {
	mu      sync.Mutex
	mode    gateMode
	blockCh chan struct{}
}

func (g *gate) waitIfStalled() {
	for {
		g.mu.Lock()
		mode := g.mode
		ch := g.blockCh
		g.mu.Unlock()
		if mode != modeStalled {
			return
		}
		<-ch
	}
}

func (g *gate) beforeMutation() syscall.Errno {
	for {
		g.mu.Lock()
		mode := g.mode
		ch := g.blockCh
		g.mu.Unlock()

		switch mode {
		case modeFull:
			return syscall.ENOSPC
		case modeStalled:
			<-ch
		default:
			return 0
		}
	}
}

func (g *gate) setMode(mode gateMode) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if mode == modeStalled {
		if g.blockCh == nil {
			g.blockCh = make(chan struct{})
		}
		g.mode = mode
		return
	}

	if g.blockCh != nil {
		close(g.blockCh)
		g.blockCh = nil
	}
	g.mode = mode
}

var ioGate = &gate{}

type gatedNode struct {
	fs.LoopbackNode
}

func newGatedNode(rootData *fs.LoopbackRoot, _ *fs.Inode, _ string, _ *syscall.Stat_t) fs.InodeEmbedder {
	return &gatedNode{LoopbackNode: fs.LoopbackNode{RootData: rootData}}
}

func (n *gatedNode) Create(ctx context.Context, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if errno := ioGate.beforeMutation(); errno != 0 {
		return nil, nil, 0, errno
	}
	inode, fh, fuseFlags, errno := n.LoopbackNode.Create(ctx, name, flags, mode, out)
	return inode, wrapFileHandle(fh), fuseFlags, errno
}

func (n *gatedNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	ioGate.waitIfStalled()
	fh, fuseFlags, errno := n.LoopbackNode.Open(ctx, flags)
	return wrapFileHandle(fh), fuseFlags, errno
}

func (n *gatedNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if errno := ioGate.beforeMutation(); errno != 0 {
		return errno
	}
	if gf, ok := f.(*gatedFile); ok {
		f = gf.LoopbackFile
	}
	return n.LoopbackNode.Setattr(ctx, f, in, out)
}

type gatedFile struct {
	*fs.LoopbackFile
}

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
	if errno := ioGate.beforeMutation(); errno != 0 {
		return 0, errno
	}
	return f.LoopbackFile.Write(ctx, data, off)
}

func (f *gatedFile) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	if errno := ioGate.beforeMutation(); errno != 0 {
		return errno
	}
	return f.LoopbackFile.Fsync(ctx, flags)
}

func SetStalled(pid int) error {
	return signalProcess(pid, signalStalled)
}

func SetFull(pid int) error {
	return signalProcess(pid, signalFull)
}

func Clear(pid int) error {
	return signalProcess(pid, signalClear)
}

func Close(pid int) error {
	return signalProcess(pid, syscall.SIGTERM)
}

func signalProcess(pid int, signal os.Signal) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return process.Signal(signal)
}

func Run(mountPoint, backing string) error {
	if mountPoint == "" || backing == "" {
		return fmt.Errorf("both mount and backing are required")
	}

	var st syscall.Stat_t
	if err := syscall.Stat(backing, &st); err != nil {
		return fmt.Errorf("stat(%s): %w", backing, err)
	}

	root := &fs.LoopbackRoot{
		Path:    backing,
		Dev:     uint64(st.Dev),
		NewNode: newGatedNode,
	}
	rootNode := newGatedNode(root, nil, "", &st)
	root.RootNode = rootNode

	server, err := fs.Mount(mountPoint, rootNode, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: backing,
			Name:   "vitess-disk-health-monitor-test",
		},
	})
	if err != nil {
		return fmt.Errorf("fs.Mount(%s): %w", mountPoint, err)
	}

	modeSig := make(chan os.Signal, 4)
	signal.Notify(modeSig, signalStalled, signalFull, signalClear)
	go func() {
		for s := range modeSig {
			switch s {
			case signalStalled:
				ioGate.setMode(modeStalled)
			case signalFull:
				ioGate.setMode(modeFull)
			case signalClear:
				ioGate.setMode(modeHealthy)
			}
		}
	}()

	termSig := make(chan os.Signal, 1)
	signal.Notify(termSig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-termSig
		ioGate.setMode(modeHealthy)
		_ = server.Unmount()
	}()

	fmt.Println("READY")
	server.Wait()
	return nil
}

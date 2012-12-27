// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package umgmt

import (
	"fmt"
	"net"
	"os"
	"syscall"
)

func SendFd(conn *net.UnixConn, file *os.File) error {
	rights := syscall.UnixRights(int(file.Fd()))
	dummy := []byte("x")
	n, oobn, err := conn.WriteMsgUnix(dummy, rights, nil)
	if err != nil {
		return fmt.Errorf("sendfd: err %v", err)
	}
	if n != len(dummy) {
		return fmt.Errorf("sendfd: short write %v", conn)
	}
	if oobn != len(rights) {
		return fmt.Errorf("sendfd: short oob write %v", conn)
	}
	return nil
}

func RecvFd(conn *net.UnixConn) (*os.File, error) {
	buf := make([]byte, 32)
	oob := make([]byte, 32)
	_, oobn, _, _, err := conn.ReadMsgUnix(buf, oob)
	if err != nil {
		return nil, fmt.Errorf("recvfd: err %v", err)
	}
	scms, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return nil, fmt.Errorf("recvfd: ParseSocketControlMessage failed %v", err)
	}
	if len(scms) != 1 {
		return nil, fmt.Errorf("recvfd: SocketControlMessage count not 1: %v", len(scms))
	}
	scm := scms[0]
	fds, err := syscall.ParseUnixRights(&scm)
	if err != nil {
		return nil, fmt.Errorf("recvfd: ParseUnixRights failed %v", err)
	}
	if len(fds) != 1 {
		return nil, fmt.Errorf("recvfd: fd count not 1: %v", len(fds))
	}
	return os.NewFile(uintptr(fds[0]), "passed-fd"), nil
}

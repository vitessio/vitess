//go:build !windows

/*
Copyright 2023 The Vitess Authors.

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

package vtgate

import (
	"syscall"

	"vitess.io/vitess/go/mysql"
)

func setupUnixSocket(srv *mysqlServer, authServer mysql.AuthServer, path string) error {
	// Let's create this unix socket with permissions to all users. In this way,
	// clients can connect to vtgate mysql server without being vtgate user
	var err error
	oldMask := syscall.Umask(000)
	srv.unixListener, err = newMysqlUnixSocket(path, authServer, srv.vtgateHandle)
	_ = syscall.Umask(oldMask)
	if err != nil {
		return err
	}
	// Listen for unix socket
	go srv.unixListener.Accept()
	return nil
}

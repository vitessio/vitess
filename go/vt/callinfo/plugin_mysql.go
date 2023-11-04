/*
Copyright 2019 The Vitess Authors.

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

package callinfo

// This file implements the CallInfo interface for Mysql contexts.

import (
	"context"
	"fmt"

	"github.com/google/safehtml"
	"github.com/google/safehtml/template"

	"vitess.io/vitess/go/mysql"
)

// MysqlCallInfo returns an augmented context with a CallInfo structure,
// only for Mysql contexts.
func MysqlCallInfo(ctx context.Context, c *mysql.Conn) context.Context {
	return NewContext(ctx, &mysqlCallInfoImpl{
		remoteAddr: c.RemoteAddr().String(),
		user:       c.User,
	})
}

type mysqlCallInfoImpl struct {
	remoteAddr string
	user       string
}

func (mci *mysqlCallInfoImpl) RemoteAddr() string {
	return mci.remoteAddr
}

func (mci *mysqlCallInfoImpl) Username() string {
	return mci.user
}

func (mci *mysqlCallInfoImpl) Text() string {
	return fmt.Sprintf("%s@%s(Mysql)", mci.user, mci.remoteAddr)
}

var mysqlTmpl = template.Must(template.New("tcs").Parse("<b>MySQL User:</b> {{.MySQLUser}} <b>Remote Addr:</b> {{.RemoteAddr}}"))

func (mci *mysqlCallInfoImpl) HTML() safehtml.HTML {
	html, err := mysqlTmpl.ExecuteToHTML(struct {
		MySQLUser  string
		RemoteAddr string
	}{
		MySQLUser:  mci.user,
		RemoteAddr: mci.remoteAddr,
	})
	if err != nil {
		panic(err)
	}
	return html
}

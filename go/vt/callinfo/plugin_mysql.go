package callinfo

// This file implements the CallInfo interface for Mysql contexts.

import (
	"fmt"
	"html/template"

	"context"

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

func (mci *mysqlCallInfoImpl) HTML() template.HTML {
	return template.HTML("<b>MySQL User:</b> " + mci.user + " <b>Remote Addr:<b> " + mci.remoteAddr)
}

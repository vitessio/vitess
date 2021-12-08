// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlparser

import (
	"fmt"
	"strings"
)

// AccountName represents either a user or role name, which has the format `name`@`host`.
type AccountName struct {
	Name    string
	Host    string
	AnyHost bool
}

// String returns the AccountName as a formatted string.
func (an *AccountName) String() string {
	host := an.Host
	if an.AnyHost {
		host = "%"
	}
	return fmt.Sprintf("`%s`@`%s`",
		strings.ReplaceAll(an.Name, "`", "``"), strings.ReplaceAll(host, "`", "``"))
}

// AccountRename represents an account changing its name.
type AccountRename struct {
	From AccountName
	To   AccountName
}

// String returns the AccountRename as a formatted string.
func (ar *AccountRename) String() string {
	return fmt.Sprintf("%s to %s", ar.From.String(), ar.To.String())
}

// Authentication represents an account's authentication.
type Authentication struct {
	RandomPassword   bool
	Password         string
	Identity         string
	Plugin           string
}

// String returns this Authentication as a formatted string.
func (auth *Authentication) String() string {
	if len(auth.Plugin) > 0 {
		if len(auth.Password) > 0 {
			return fmt.Sprintf("identified with %s by '%s'", auth.Plugin, auth.Password)
		} else if auth.RandomPassword {
			return fmt.Sprintf("identified with %s by random password", auth.Plugin)
		} else if len(auth.Identity) > 0 {
			return fmt.Sprintf("identified with %s as '%s'", auth.Plugin, auth.Identity)
		} else {
			return fmt.Sprintf("identified with %s", auth.Plugin)
		}
	} else if auth.RandomPassword {
		return "identified by random password"
	} else {
		return fmt.Sprintf("identified by '%s'", auth.Password)
	}
}

// AccountWithAuth represents a new account with all of its authentication information.
type AccountWithAuth struct {
	AccountName
	Auth1       *Authentication
	Auth2       *Authentication
	Auth3       *Authentication
	AuthInitial *Authentication
}

// String returns AccountWithAuth as a formatted string.
func (awa *AccountWithAuth) String() string {
	sb := strings.Builder{}
	sb.WriteString(awa.AccountName.String())
	if awa.Auth1 != nil {
		sb.WriteRune(' ')
		sb.WriteString(awa.Auth1.String())
		if awa.AuthInitial != nil {
			sb.WriteString(" initial authentication ")
			sb.WriteString(awa.AuthInitial.String())
		} else if awa.Auth2 != nil {
			sb.WriteString(" and ")
			sb.WriteString(awa.Auth2.String())
			if awa.Auth3 != nil {
				sb.WriteString(" and ")
				sb.WriteString(awa.Auth3.String())
			}
		}
	}
	return sb.String()
}

// TLSOptions represents a new user's TLS options.
type TLSOptions struct {
	SSL bool
	X509 bool
	Cipher string
	Issuer string
	Subject string
}

// String returns the TLSOptions as a formatted string.
func (tls *TLSOptions) String() string {
	var options []string
	if tls.SSL {
		options = append(options, "SSL")
	}
	if tls.X509 {
		options = append(options, "X509")
	}
	if len(tls.Cipher) > 0 {
		options = append(options, fmt.Sprintf("cipher '%s'", tls.Cipher))
	}
	if len(tls.Issuer) > 0 {
		options = append(options, fmt.Sprintf("issuer '%s'", tls.Issuer))
	}
	if len(tls.Subject) > 0 {
		options = append(options, fmt.Sprintf("subject '%s'", tls.Subject))
	}
	return strings.Join(options, " AND ")
}

// AccountLimits represents a new user's maximum limits.
type AccountLimits struct {
	MaxQueriesPerHour uint64
	MaxUpdatesPerHour uint64
	MaxConnectionsPerHour uint64
	MaxUserConnections uint64
}

// String returns the AccountLimits as a formatted string.
func (al *AccountLimits) String() string {
	var limits []string
	if al.MaxQueriesPerHour > 0 {
		limits = append(limits, fmt.Sprintf("max_queries_per_hour %d", al.MaxQueriesPerHour))
	}
	if al.MaxUpdatesPerHour > 0 {
		limits = append(limits, fmt.Sprintf("max_updates_per_hour %d", al.MaxUpdatesPerHour))
	}
	if al.MaxConnectionsPerHour > 0 {
		limits = append(limits, fmt.Sprintf("max_connections_per_hour %d", al.MaxConnectionsPerHour))
	}
	if al.MaxUserConnections > 0 {
		limits = append(limits, fmt.Sprintf("max_user_connections %d", al.MaxUserConnections))
	}
	return strings.Join(limits, " ")
}

// PasswordOptions represents which options may be given to new user account on how to handle passwords.
type PasswordOptions struct {
	ExpirationDefault bool
	HistoryDefault bool
	ReuseDefault bool
	RequireCurrentDefault bool
	LockTimeUnbounded bool

	ExpirationTime uint64
	History uint64
	ReuseInterval uint64
	FailedAttempts uint64
	LockTime uint64
}

// String returns PasswordOptions as a formatted string.
func (po *PasswordOptions) String() string {
	var options []string
	if !po.ExpirationDefault {
		if po.ExpirationTime == 0 {
			options = append(options, "password expire never")
		} else {
			options = append(options, fmt.Sprintf("password expire interval %d day", po.ExpirationTime))
		}
	}
	if !po.HistoryDefault {
		options = append(options, fmt.Sprintf("password history %d", po.History))
	}
	if !po.ReuseDefault {
		options = append(options, fmt.Sprintf("password reuse interval %d day", po.ReuseInterval))
	}
	if !po.RequireCurrentDefault {
		options = append(options, "password require current optional")
	}
	if po.FailedAttempts > 0 {
		options = append(options, fmt.Sprintf("failed_login_attempts %d", po.FailedAttempts))
	}
	if po.LockTimeUnbounded {
		options = append(options, "password_lock_time unbounded")
	} else if po.LockTime > 0 {
		options = append(options, fmt.Sprintf("password_lock_time %d", po.LockTime))
	}
	return strings.Join(options, " ")
}

// CreateUser represents the CREATE USER statement.
type CreateUser struct {
	IfNotExists bool
	Users []AccountWithAuth
	DefaultRoles []AccountName
	TLSOptions *TLSOptions
	AccountLimits *AccountLimits
	PasswordOptions *PasswordOptions
	Locked bool
	Attribute string
}

var _ Statement = (*CreateUser)(nil)

// iStatement implements the interface Statement.
func (c *CreateUser) iStatement() {}

// Format implements the interface Statement.
func (c *CreateUser) Format(buf *TrackedBuffer) {
	if c.IfNotExists {
		buf.Myprintf("create user if not exists")
	} else {
		buf.Myprintf("create user")
	}
	for i, user := range c.Users {
		if i > 0 {
			buf.Myprintf(",")
		}
		buf.Myprintf(" %s", user.String())
	}
	if len(c.DefaultRoles) > 0 {
		buf.Myprintf(" default role")
		for i, role := range c.DefaultRoles {
			if i > 0 {
				buf.Myprintf(",")
			}
			buf.Myprintf(" %s", role.String())
		}
	}
	if c.TLSOptions != nil {
		buf.Myprintf(" require ")
		buf.Myprintf(c.TLSOptions.String())
	}
	if c.AccountLimits != nil {
		buf.Myprintf(" with ")
		buf.Myprintf(c.AccountLimits.String())
	}
	if c.PasswordOptions != nil {
		buf.Myprintf(c.PasswordOptions.String())
	}
	if c.Locked {
		buf.Myprintf(" account lock")
	}
	if len(c.Attribute) > 0 {
		buf.Myprintf(" %s", c.Attribute)
	}
}

// walkSubtree implements the interface Statement.
func (c *CreateUser) walkSubtree(visit Visit) error {
	return nil
}

// RenameUser represents the RENAME USER statement.
type RenameUser struct {
	Accounts []AccountRename
}

var _ Statement = (*RenameUser)(nil)

// iStatement implements the interface Statement.
func (r *RenameUser) iStatement() {}

// Format implements the interface Statement.
func (r *RenameUser) Format(buf *TrackedBuffer) {
	buf.Myprintf("rename user")
	for i, accountRename := range r.Accounts {
		if i > 0 {
			buf.Myprintf(",")
		}
		buf.Myprintf(" %s", accountRename.String())
	}
}

// walkSubtree implements the interface Statement.
func (r *RenameUser) walkSubtree(visit Visit) error {
	return nil
}

// DropUser represents the DROP USER statement.
type DropUser struct {
	IfExists bool
	AccountNames []AccountName
}

var _ Statement = (*DropUser)(nil)

// iStatement implements the interface Statement.
func (d *DropUser) iStatement() {}

// Format implements the interface Statement.
func (d *DropUser) Format(buf *TrackedBuffer) {
	if d.IfExists {
		buf.Myprintf("drop user if exists")
	} else {
		buf.Myprintf("drop user")
	}
	for i, an := range d.AccountNames {
		if i > 0 {
			buf.Myprintf(",")
		}
		buf.Myprintf(" %s", an.String())
	}
}

// walkSubtree implements the interface Statement.
func (d *DropUser) walkSubtree(visit Visit) error {
	return nil
}

// CreateRole represents the CREATE ROLE statement.
type CreateRole struct {
	IfNotExists bool
	Roles       []AccountName
}

var _ Statement = (*CreateRole)(nil)

// iStatement implements the interface Statement.
func (c *CreateRole) iStatement() {}

// Format implements the interface Statement.
func (c *CreateRole) Format(buf *TrackedBuffer) {
	if c.IfNotExists {
		buf.Myprintf("create role if not exists")
	} else {
		buf.Myprintf("create role")
	}
	for i, role := range c.Roles {
		if i > 0 {
			buf.Myprintf(",")
		}
		buf.Myprintf(" %s", role.String())
	}
}

// walkSubtree implements the interface Statement.
func (c *CreateRole) walkSubtree(visit Visit) error {
	return nil
}

// DropRole represents the DROP ROLE statement.
type DropRole struct {
	IfExists bool
	Roles    []AccountName
}

var _ Statement = (*DropRole)(nil)

// iStatement implements the interface Statement.
func (d *DropRole) iStatement() {}

// Format implements the interface Statement.
func (d *DropRole) Format(buf *TrackedBuffer) {
	if d.IfExists {
		buf.Myprintf("drop role if exists")
	} else {
		buf.Myprintf("drop role")
	}
	for i, role := range d.Roles {
		if i > 0 {
			buf.Myprintf(",")
		}
		buf.Myprintf(" %s", role.String())
	}
}

// walkSubtree implements the interface Statement.
func (d *DropRole) walkSubtree(visit Visit) error {
	return nil
}

// ShowGrants represents the SHOW GRANTS statement.
type ShowGrants struct {
	CurrentUser bool
	For         *AccountName
	Using       []AccountName
}

var _ Statement = (*ShowGrants)(nil)

// iStatement implements the interface Statement.
func (s *ShowGrants) iStatement() {}

// Format implements the interface Statement.
func (s *ShowGrants) Format(buf *TrackedBuffer) {
	buf.Myprintf("show grants")
	if s.CurrentUser || s.For != nil {
		if s.CurrentUser {
			buf.Myprintf(" for Current_User()")
		} else {
			buf.Myprintf(" for %s", s.For.String())
		}
		if len(s.Using) > 0 {
			buf.Myprintf(" using")
			for i, using := range s.Using {
				if i > 0 {
					buf.Myprintf(",")
				}
				buf.Myprintf(" %s", using.String())
			}
		}
	}
}

// walkSubtree implements the interface Statement.
func (s *ShowGrants) walkSubtree(visit Visit) error {
	return nil
}

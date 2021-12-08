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
	Name string
	Host string
}

// String returns the AccountName as a formatted string.
func (an AccountName) String() string {
	return fmt.Sprintf("`%s`@`%s`",
		strings.ReplaceAll(an.Name, "`", "``"), strings.ReplaceAll(an.Host, "`", "``"))
}

// DropUser represents the DROP USER statement.
type DropUser struct {
	IfExists bool
	AccountNames []AccountName
}

var _ Statement = (*DropUser)(nil)

// iStatement implements the interface Statement.
func (d DropUser) iStatement() {}

// Format implements the interface Statement.
func (d DropUser) Format(buf *TrackedBuffer) {
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
func (d DropUser) walkSubtree(visit Visit) error {
	return nil
}

// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fakecallinfo

import "html/template"

// FakeCallInfo gives a fake Callinfo usable in callinfo
type FakeCallInfo struct {
	Remote string
	User   string
	Txt    string
	Html   string
}

// RemoteAddr returns the remote address.
func (fci *FakeCallInfo) RemoteAddr() string {
	return fci.Remote
}

// Username returns the user name.
func (fci *FakeCallInfo) Username() string {
	return fci.User
}

// Text returns the text.
func (fci *FakeCallInfo) Text() string {
	return fci.Txt
}

// HTML returns the html.
func (fci *FakeCallInfo) HTML() template.HTML {
	return template.HTML(fci.Html)
}

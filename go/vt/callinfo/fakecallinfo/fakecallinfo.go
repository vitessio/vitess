/*
Copyright 2017 Google Inc.

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

package fakecallinfo

import (
	"fmt"
	"html/template"
)

// FakeCallInfo gives a fake Callinfo usable in callinfo
type FakeCallInfo struct {
	Remote string
	Method string
	User   string
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
	return fmt.Sprintf("%s:%s(fakeRPC)", fci.Remote, fci.Method)
}

// HTML returns the html.
func (fci *FakeCallInfo) HTML() template.HTML {
	return template.HTML(fci.Html)
}

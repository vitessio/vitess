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

package debug

import "time"

// Debuggable is an optional interface that different vtadmin subcomponents can
// implement to provide information to debug endpoints.
type Debuggable interface {
	Debug() map[string]interface{}
}

const sanitized = "********"

// SanitizeString returns a sanitized version of the input string. If the input
// is empty, an empty string is returned. Otherwise a constant-length string
// (irrespective of the input length) is returned.
func SanitizeString(s string) string {
	if len(s) == 0 {
		return s
	}

	return sanitized
}

// TimeToString returns a time in RFC3339-formatted string, so all debug-related
// times are in the same format.
func TimeToString(t time.Time) string { return t.Format(time.RFC3339) }

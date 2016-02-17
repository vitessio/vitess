package explorer

import "net/http"

// Result is what the explorer returns. It represents one directory node.
type Result struct {
	Data     string
	Children []string
	Error    string
}

// Explorer allows exploring a topology server.
type Explorer interface {
	// HandlePath returns a Result for the given path.
	HandlePath(url string, r *http.Request) *Result
}

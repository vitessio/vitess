package vtadmin

import "errors"

var (
	// ErrAmbiguousTablet occurs when more than one tablet is found for a given
	// set of filter criteria.
	ErrAmbiguousTablet = errors.New("multiple tablets found")
	// ErrNoTablet occurs when a tablet cannot be found for a given set of
	// filter criteria.
	ErrNoTablet = errors.New("no such tablet")
	// ErrUnsupportedCluster occurs when a cluster parameter is invalid.
	ErrUnsupportedCluster = errors.New("unsupported cluster(s)")
)

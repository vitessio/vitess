package errors

import (
	"errors"
	"fmt"
)

// Errors returned by API endpoints.
var (
	// ErrAmbiguousSchema occurs when more than one schema is found for a given
	// set of filter criteria.
	ErrAmbiguousSchema = errors.New("multiple schemas found")
	// ErrAmbiguousTablet occurs when more than one tablet is found for a given
	// set of filter criteria.
	ErrAmbiguousTablet = errors.New("multiple tablets found")
	// ErrAmbiguousWorkflow occurs when more than one workflow is found for a
	// set of filter criteria that should ordinarily never return more than one
	// workflow.
	ErrAmbiguousWorkflow = errors.New("multiple workflows found")
	// ErrInvalidRequest occurs when a request is invalid for any reason.
	// For example, if mandatory parameters are undefined.
	ErrInvalidRequest = errors.New("Invalid request")
	// ErrNoServingTablet occurs when a tablet with state SERVING cannot be
	// found for a given set of filter criteria. It is a more specific form of
	// ErrNoTablet
	ErrNoServingTablet = fmt.Errorf("%w with state=SERVING", ErrNoTablet)
	// ErrNoSrvVSchema occurs when no SrvVSchema is found for a given keyspace.
	ErrNoSrvVSchema = errors.New("SrvVSchema not found")
	// ErrNoTablet occurs when a tablet cannot be found for a given set of
	// filter criteria.
	ErrNoTablet = errors.New("no such tablet")
	// ErrNoWorkflow occurs when a workflow cannot be found for a given set of
	// filter criteria.
	ErrNoWorkflow = errors.New("no such workflow")
	// ErrUnauthorized occurs when attempting to perform a (subject, resource, action)
	// in a cluster that the rbac configuration does not allow.
	ErrUnauthorized = errors.New("unauthorized")
	// ErrUnsupportedCluster occurs when a cluster parameter is invalid.
	ErrUnsupportedCluster = errors.New("unsupported cluster(s)")
)

// Errors returned by cluster setup and flag parsing.
var (
	// ErrNoFlag occurs when cluster config parsing encounters a flag specified
	// in the DSN that is not defined.
	ErrNoFlag = errors.New("flag provided but not defined")
)

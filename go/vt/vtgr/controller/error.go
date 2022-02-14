package controller

import "errors"

var (
	errMissingPrimaryTablet = errors.New("no primary tablet available")
	errMissingGroup         = errors.New("no mysql group")
	errForceAbortBootstrap  = errors.New("force abort bootstrap")
)

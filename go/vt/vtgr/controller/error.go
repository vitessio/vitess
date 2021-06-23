package controller

import "errors"

var (
	errMissingPrimaryTablet = errors.New("no_primary_tablet_available")
	errMissingGroup         = errors.New("no_mysql_group")
	errForceAbortBootstrap  = errors.New("force abort bootstrap")
)

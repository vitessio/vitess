package viperutil

import (
	"vitess.io/vitess/go/viperutil/v2/internal/sync"
	"vitess.io/vitess/go/viperutil/v2/internal/value"
)

var (
	ErrDuplicateWatch = sync.ErrDuplicateWatch
	ErrNoFlagDefined  = value.ErrNoFlagDefined
)

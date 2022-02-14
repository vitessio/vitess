package onlineddl

import (
	"vitess.io/vitess/go/stats"
)

var (
	startedMigrations    = stats.NewCounter("StartedMigrations", "Count of initiated migrations")
	successfulMigrations = stats.NewCounter("SuccessfulMigrations", "Count of successful migrations, a subset of StartedMigrations")
	failedMigrations     = stats.NewCounter("FailedMigrations", "Count of failed migrations, a subset of StartedMigrations")
)

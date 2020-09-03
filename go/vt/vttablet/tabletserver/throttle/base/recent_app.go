package base

import (
	"time"
)

// RecentApp indicates when an app was last checked
type RecentApp struct {
	CheckedAtEpoch      int64
	MinutesSinceChecked int64
}

// NewRecentApp creates a RecentApp
func NewRecentApp(checkedAt time.Time) *RecentApp {
	result := &RecentApp{
		CheckedAtEpoch:      checkedAt.Unix(),
		MinutesSinceChecked: int64(time.Since(checkedAt).Minutes()),
	}
	return result
}

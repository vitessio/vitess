package base

import (
	"time"
)

// AppThrottle is the definition for an app throttling instruction
// - Ratio: [0..1], 0 == no throttle, 1 == fully throttle
type AppThrottle struct {
	AppName  string
	ExpireAt time.Time
	Ratio    float64
}

// NewAppThrottle creates an AppThrottle struct
func NewAppThrottle(appName string, expireAt time.Time, ratio float64) *AppThrottle {
	result := &AppThrottle{
		AppName:  appName,
		ExpireAt: expireAt,
		Ratio:    ratio,
	}
	return result
}

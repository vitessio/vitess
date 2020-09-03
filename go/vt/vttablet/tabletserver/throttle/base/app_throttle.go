package base

import (
	"time"
)

// AppThrottle is the definition for an app throtting instruction
// - Ratio: [0..1], 0 == no throttle, 1 == fully throttle
type AppThrottle struct {
	ExpireAt time.Time
	Ratio    float64
}

// NewAppThrottle creates an AppThrottle struct
func NewAppThrottle(expireAt time.Time, ratio float64) *AppThrottle {
	result := &AppThrottle{
		ExpireAt: expireAt,
		Ratio:    ratio,
	}
	return result
}

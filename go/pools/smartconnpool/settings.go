package smartconnpool

import (
	"sync/atomic"
)

type Setting struct {
	queryApply string
	queryReset string
	bucket     uint32
}

func (s *Setting) ApplyQuery() string {
	return s.queryApply
}

func (s *Setting) ResetQuery() string {
	return s.queryReset
}

var globalSettingsCounter atomic.Uint32

func NewSetting(apply, reset string) *Setting {
	return &Setting{apply, reset, globalSettingsCounter.Add(1)}
}

package thirdparty

import (
	"sync"
)

var once sync.Once

func InitializeThirdParty() {
	// only initialize once
	once.Do(func() {
		// initialize/register any third part implemenations

	})
}

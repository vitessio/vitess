package thirdparty

import (
	"sync"

	"github.com/spf13/cobra"
)

var once sync.Once

func InitializeThirdParty(command *cobra.Command) {
	// only initialize once
	once.Do(func() {
		// initialize/register any third part implementations

	})
}

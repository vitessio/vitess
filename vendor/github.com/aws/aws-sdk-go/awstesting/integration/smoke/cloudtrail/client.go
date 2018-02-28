//Package cloudtrail provides gucumber integration tests support.
package cloudtrail

import (
	"github.com/aws/aws-sdk-go/awstesting/integration/smoke"
	"github.com/aws/aws-sdk-go/service/cloudtrail"
	. "github.com/lsegal/gucumber"
)

func init() {
	Before("@cloudtrail", func() {
		World["client"] = cloudtrail.New(smoke.Session)
	})
}

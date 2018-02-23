//Package applicationdiscoveryservice provides gucumber integration tests support.
package applicationdiscoveryservice

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/awstesting/integration/smoke"
	"github.com/aws/aws-sdk-go/service/applicationdiscoveryservice"
	. "github.com/lsegal/gucumber"
)

func init() {
	Before("@applicationdiscoveryservice", func() {
		World["client"] = applicationdiscoveryservice.New(smoke.Session, &aws.Config{Region: aws.String("us-west-2")})
	})
}

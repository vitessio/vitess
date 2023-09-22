/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package servenv

import (
	"context"
	"crypto/x509"
	"net/url"
	"strings"

	"github.com/spf13/pflag"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"vitess.io/vitess/go/vt/log"
)

var (
	// spiffeTrustDomains list of allowed SPIFFE Trust Domains for client SVIDs during authorization
	spiffeTrustDomains string
	// SPIFFEAuthPlugin implements AuthPlugin interface
	_ Authenticator = (*SPIFFEAuthPlugin)(nil)
)

// The datatype for spiffe auth Context keys
type spiffeIdKey int

const (
	// Internal Context key for the authenticated SPIFFE ID
	spiffeId spiffeIdKey = 0
)

func registerGRPCServerAuthSPIFFEFlags(fs *pflag.FlagSet) {
	fs.StringVar(&spiffeTrustDomains, "grpc_auth_spiffe_allowed_trust_domains", spiffeTrustDomains, "List of allowed SPIFFE Trust Domains for client SVIDs (separated by comma).")
}

// SPIFFEAuthPlugin implements X.509-based SVID for SPIFFE authentication for grpc. It contains an array of trust domains
// that will be authorized to connect to the grpc server.
type SPIFFEAuthPlugin struct {
	spiffeTrustDomains []string
}

// Authenticate implements Authenticator interface. This method will be used inside a middleware in grpc_server to authenticate
// incoming requests.
func (spa *SPIFFEAuthPlugin) Authenticate(ctx context.Context, fullMethod string) (context.Context, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "no peer connection info")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "not connected via TLS")
	}

	cert := tlsInfo.State.PeerCertificates[0] // Only check the leaf certificate
	spiffeIdUrl, ok := validateSVIDCert(cert, spa.spiffeTrustDomains)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "client certificate not authorized")
	}

	log.Infof("SPIFFE auth plugin has authenticated client with SPIFFE ID %v", spiffeId)
	return newSPIFFEAuthContext(ctx, spiffeIdUrl), nil
}

// Validates the given certificate as a valid SVID leaf certificate based on
// https://github.com/spiffe/spiffe/blob/main/standards/X509-SVID.md#4-constraints-and-usage
// and https://github.com/spiffe/spiffe/blob/main/standards/X509-SVID.md#5-validation
func validateSVIDCert(cert *x509.Certificate, trustedDomains []string) (*url.URL, bool) {
	issuedForTrustedDomain := false

	// Leaf SVIDs should have exactly one URI SAN
	if len(cert.URIs) != 1 {
		return nil, false
	}

	if cert.URIs[0].Scheme != "spiffe" {
		return nil, false
	}

	for _, trustDomain := range trustedDomains {
		if cert.URIs[0].Hostname() == trustDomain {
			issuedForTrustedDomain = true
			break
		}
	}

	if !issuedForTrustedDomain {
		return nil, false
	}

	// Leaf SVIDs should not be CA certs
	if cert.IsCA {
		return nil, false
	}

	// Leaf SVIDs should not have CA usage
	if cert.KeyUsage&x509.KeyUsageCertSign != 0 {
		return nil, false
	}

	// Leaf SVIDs should not have CRL signing usage
	if cert.KeyUsage&x509.KeyUsageCRLSign != 0 {
		return nil, false
	}

	// Leaf SVIDs must have Digital Signature usage
	if cert.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		return nil, false
	}

	return cert.URIs[0], true
}

func newSPIFFEAuthContext(ctx context.Context, foundSPIFFEId *url.URL) context.Context {
	return context.WithValue(ctx, spiffeId, foundSPIFFEId)
}

func spiffeAuthPluginInitializer() (Authenticator, error) {
	spiffeAuthPlugin := &SPIFFEAuthPlugin{
		spiffeTrustDomains: strings.Split(spiffeTrustDomains, ","),
	}
	log.Infof("SPIFFE auth plugin has initialized successfully with allowed trust domains of %v", spiffeTrustDomains)
	return spiffeAuthPlugin, nil
}

// SPIFFEIdFromContext returns the SPIFFE ID authenticated by the spiffe auth plugin and stored in the Context, if any
func SPIFFEIdFromContext(ctx context.Context) *url.URL {
	spiffeId, ok := ctx.Value(spiffeId).(*url.URL)
	if ok {
		return spiffeId
	}
	return nil
}

// SPIFFETrustDomains returns the value of the
// `--grpc_auth_spiffe_allowed_trust_domains` flag.
func SPIFFETrustDomains() string {
	return spiffeTrustDomains
}

func init() {
	RegisterAuthPlugin("spiffe", spiffeAuthPluginInitializer)
	grpcAuthServerFlagHooks = append(grpcAuthServerFlagHooks, registerGRPCServerAuthSPIFFEFlags)
}

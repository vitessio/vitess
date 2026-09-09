/*
Copyright 2021 The Vitess Authors.

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

package vttls

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"time"

	"vitess.io/vitess/go/vt/logutil"
)

// maxIssuerSignatureChecks bounds the signature verifications spent
// per connection on looking for the issuers of the certificates the
// peer presented, whose number and names the peer controls. It
// mirrors the bound that crypto/x509 puts on chain building.
const maxIssuerSignatureChecks = 100

// expiredCRLLogger throttles the warning about an expired CRL, which
// would otherwise repeat on every handshake that consults it.
var expiredCRLLogger = logutil.NewThrottledLogger("vttls-expired-crl", time.Minute)

type (
	// crlChecker rejects a connection whose peer presents a
	// certificate listed in one of the configured Certificate
	// Revocation Lists.
	crlChecker struct {
		crls []*x509.RevocationList
		// issuers are the CA certificates configured for the
		// connection. A CRL only applies to a certificate when the
		// CRL's signature verifies from that certificate's issuer,
		// and a peer commonly presents its leaf certificate alone,
		// so the issuer is looked for here as well as among the
		// certificates the peer sent.
		issuers []*x509.Certificate
		// anchors holds the DER encoding of the configured
		// issuers, which are trust anchors: their own revocation
		// is not checked, as it never was.
		anchors map[string]struct{}
	}

	// crlCheck is the state of one connection's revocation check.
	crlCheck struct {
		checker *crlChecker
		// bySubject indexes the certificates that may be an
		// issuer by subject, the configured ones first, then the
		// ones from verified chains, then the presented ones.
		bySubject       map[string][]*x509.Certificate
		indexed         map[string]struct{}
		crlsByIssuer    map[*x509.Certificate][]*x509.RevocationList
		signatureChecks int
	}
)

func certIsRevoked(cert *x509.Certificate, crl *x509.RevocationList) bool {
	if !time.Now().Before(crl.NextUpdate) {
		expiredCRLLogger.Warningf("The current Certificate Revocation List (CRL) is past expiry date and must be updated. Revoked certificates will still be rejected in this state.")
	}

	for _, revoked := range crl.RevokedCertificateEntries {
		if cert.SerialNumber.Cmp(revoked.SerialNumber) == 0 {
			return true
		}
	}
	return false
}

// newCRLChecker loads the CRLs in crl and, when ca is set, the CA
// certificates that the CRLs may be signed by.
func newCRLChecker(crl, ca string) (*crlChecker, error) {
	crls, err := loadCRLSet(crl)
	if err != nil {
		return nil, err
	}
	checker := &crlChecker{crls: crls, anchors: map[string]struct{}{}}
	if ca != "" {
		checker.issuers, err = loadx509Certificates(ca)
		if err != nil {
			return nil, err
		}
	}
	for _, issuer := range checker.issuers {
		checker.anchors[string(issuer.Raw)] = struct{}{}
	}
	return checker, nil
}

// verifyConnection is a tls.Config.VerifyConnection callback. Unlike
// VerifyPeerCertificate, Go runs it on every connection: it is not
// skipped on resumed sessions, and it sees the peer's certificates
// even when InsecureSkipVerify disables Go's own chain verification,
// which is the case for every client mode short of verify_identity.
func (c *crlChecker) verifyConnection(cs tls.ConnectionState) error {
	return c.check(cs.PeerCertificates, cs.VerifiedChains)
}

// check walks the verified chains and then the presented chain in
// the order the certificates were sent, and rejects the connection
// when a certificate below a trust anchor is listed in a CRL signed
// by its issuer. The issuer of the leaf certificate has to be found,
// so that a peer cannot dodge the check by leaving its chain out;
// other certificates whose issuer is not available go unchecked, as
// they always did.
func (c *crlChecker) check(presented []*x509.Certificate, verifiedChains [][]*x509.Certificate) error {
	if len(presented) == 0 {
		return nil
	}
	check := c.newCheck(presented, verifiedChains)
	chains := make([][]*x509.Certificate, 0, len(verifiedChains)+1)
	chains = append(chains, verifiedChains...)
	chains = append(chains, presented)
	checked := make(map[string]struct{}, len(presented))
	for chainIndex, chain := range chains {
		verified := chainIndex < len(verifiedChains)
		for i, cert := range chain {
			if _, done := checked[string(cert.Raw)]; done {
				continue
			}
			checked[string(cert.Raw)] = struct{}{}
			if c.isAnchor(cert) || (verified && i == len(chain)-1) {
				continue
			}
			var next *x509.Certificate
			if i+1 < len(chain) {
				next = chain[i+1]
			}
			issuer := check.findIssuer(cert, next)
			if issuer == nil {
				if bytes.Equal(cert.Raw, presented[0].Raw) {
					return fmt.Errorf("cannot check the revocation of certificate CommonName=%v: no certificate is available for its issuer %v", cert.Subject.CommonName, cert.Issuer.CommonName)
				}
				continue
			}
			for _, crl := range check.crlsSignedBy(issuer) {
				if certIsRevoked(cert, crl) {
					return fmt.Errorf("Certificate revoked: CommonName=%v", cert.Subject.CommonName)
				}
			}
		}
	}
	return nil
}

// isAnchor reports whether cert is a configured issuer or is
// self-signed, neither of which has an issuer to check it against.
func (c *crlChecker) isAnchor(cert *x509.Certificate) bool {
	if _, ok := c.anchors[string(cert.Raw)]; ok {
		return true
	}
	return bytes.Equal(cert.RawIssuer, cert.RawSubject) &&
		cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature) == nil
}

func (c *crlChecker) newCheck(presented []*x509.Certificate, verifiedChains [][]*x509.Certificate) *crlCheck {
	check := &crlCheck{
		checker:      c,
		bySubject:    map[string][]*x509.Certificate{},
		indexed:      map[string]struct{}{},
		crlsByIssuer: map[*x509.Certificate][]*x509.RevocationList{},
	}
	for _, issuer := range c.issuers {
		check.index(issuer)
	}
	for _, chain := range verifiedChains {
		for _, cert := range chain {
			check.index(cert)
		}
	}
	for _, cert := range presented {
		check.index(cert)
	}
	return check
}

func (ck *crlCheck) index(cert *x509.Certificate) {
	if _, done := ck.indexed[string(cert.Raw)]; done {
		return
	}
	ck.indexed[string(cert.Raw)] = struct{}{}
	ck.bySubject[string(cert.RawSubject)] = append(ck.bySubject[string(cert.RawSubject)], cert)
}

// findIssuer returns the certificate that issued cert, trying next,
// the certificate that follows it in its chain, before the other
// candidates that carry the issuer's name.
func (ck *crlCheck) findIssuer(cert, next *x509.Certificate) *x509.Certificate {
	if next != nil && ck.issuedBy(cert, next) {
		return next
	}
	for _, candidate := range ck.bySubject[string(cert.RawIssuer)] {
		if candidate != next && ck.issuedBy(cert, candidate) {
			return candidate
		}
	}
	return nil
}

// issuedBy reports whether issuer signed cert, spending one of the
// connection's bounded signature checks when the names match.
func (ck *crlCheck) issuedBy(cert, issuer *x509.Certificate) bool {
	if !bytes.Equal(cert.RawIssuer, issuer.RawSubject) {
		return false
	}
	if ck.signatureChecks >= maxIssuerSignatureChecks {
		return false
	}
	ck.signatureChecks++
	return cert.CheckSignatureFrom(issuer) == nil
}

// crlsSignedBy returns the CRLs whose signature verifies from issuer,
// checking each CRL once per issuer for the connection.
func (ck *crlCheck) crlsSignedBy(issuer *x509.Certificate) []*x509.RevocationList {
	if crls, done := ck.crlsByIssuer[issuer]; done {
		return crls
	}
	var crls []*x509.RevocationList
	for _, crl := range ck.checker.crls {
		if crl.CheckSignatureFrom(issuer) == nil {
			crls = append(crls, crl)
		}
	}
	ck.crlsByIssuer[issuer] = crls
	return crls
}

func loadCRLSet(crl string) ([]*x509.RevocationList, error) {
	body, err := os.ReadFile(crl)
	if err != nil {
		return nil, err
	}

	crlSet := make([]*x509.RevocationList, 0)
	for len(body) > 0 {
		var block *pem.Block
		block, body = pem.Decode(body)
		if block == nil {
			break
		}
		if block.Type != "X509 CRL" {
			continue
		}

		parsedCRL, err := x509.ParseRevocationList(block.Bytes)
		if err != nil {
			return nil, err
		}
		crlSet = append(crlSet, parsedCRL)
	}
	return crlSet, nil
}

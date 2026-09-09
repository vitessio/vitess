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

// maxSignatureChecks bounds the signature verifications spent per
// connection on finding the issuers of the peer's certificates and on
// binding the CRLs to them, since the peer controls the number and
// the names of the certificates it presents. It mirrors the bound
// that crypto/x509 puts on chain building.
const maxSignatureChecks = 100

var errSignatureChecksSpent = fmt.Errorf("checking it exceeded the %d signature checks allowed per connection", maxSignatureChecks)

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
		checker   *crlChecker
		presented []*x509.Certificate
		// chains are the certificate chains the check walks, and
		// verified tells whether they were built by verification,
		// in which case their last certificate is a trust anchor.
		chains   [][]*x509.Certificate
		verified bool
		// bySubject indexes the certificates that may be an
		// issuer by subject, the configured ones first.
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

// check rejects the connection when a certificate below a trust
// anchor is listed in a CRL signed by its issuer, walking the
// verified chains when there are any and the presented chain
// otherwise, see newCheck.
func (c *crlChecker) check(presented []*x509.Certificate, verifiedChains [][]*x509.Certificate) error {
	if len(presented) == 0 {
		return nil
	}
	return c.newCheck(presented, verifiedChains).run()
}

// newCheck prepares the check of the verified chains, when
// verification built any: the certificates a peer presents beyond
// them play no part in the trust decision, so they are not inspected
// and cannot be used to make the check expensive. Only when there is
// no verified chain, because verification is disabled, is the
// presented chain checked as it is.
func (c *crlChecker) newCheck(presented []*x509.Certificate, verifiedChains [][]*x509.Certificate) *crlCheck {
	check := &crlCheck{
		checker:      c,
		presented:    presented,
		chains:       verifiedChains,
		verified:     true,
		bySubject:    map[string][]*x509.Certificate{},
		indexed:      map[string]struct{}{},
		crlsByIssuer: map[*x509.Certificate][]*x509.RevocationList{},
	}
	if len(verifiedChains) == 0 {
		check.chains = [][]*x509.Certificate{presented}
		check.verified = false
	}
	for _, issuer := range c.issuers {
		check.index(issuer)
	}
	for _, chain := range check.chains {
		for _, cert := range chain {
			check.index(cert)
		}
	}
	return check
}

// run walks the chains in the order the certificates were sent. The
// trust anchors are the configured issuers and the last certificate
// of each verified chain; a self-signed certificate the peer presents
// is not one, so that recognizing it costs one of the bounded
// signature checks like any other issuer lookup. The issuer of the
// leaf certificate has to be found, so that a peer cannot dodge the
// check by leaving its chain out; other certificates whose issuer is
// not available go unchecked, as they always did.
func (ck *crlCheck) run() error {
	checked := make(map[string]struct{}, len(ck.presented))
	for _, chain := range ck.chains {
		for i, cert := range chain {
			if _, done := checked[string(cert.Raw)]; done {
				continue
			}
			checked[string(cert.Raw)] = struct{}{}
			if _, anchor := ck.checker.anchors[string(cert.Raw)]; anchor || (ck.verified && i == len(chain)-1) {
				continue
			}
			var next *x509.Certificate
			if i+1 < len(chain) {
				next = chain[i+1]
			}
			issuer, err := ck.findIssuer(cert, next)
			if err != nil {
				return fmt.Errorf("cannot check the revocation of certificate CommonName=%v: %w", cert.Subject.CommonName, err)
			}
			if issuer == nil {
				if bytes.Equal(cert.Raw, ck.presented[0].Raw) {
					return fmt.Errorf("cannot check the revocation of certificate CommonName=%v: no certificate is available for its issuer %v", cert.Subject.CommonName, cert.Issuer.CommonName)
				}
				continue
			}
			crls, err := ck.crlsSignedBy(issuer)
			if err != nil {
				return fmt.Errorf("cannot check the revocation of certificate CommonName=%v: %w", cert.Subject.CommonName, err)
			}
			for _, crl := range crls {
				if certIsRevoked(cert, crl) {
					return fmt.Errorf("Certificate revoked: CommonName=%v", cert.Subject.CommonName)
				}
			}
		}
	}
	return nil
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
// candidates that carry the issuer's name. It returns an error, and
// not merely no issuer, when the connection's signature checks run
// out, so that a padded chain fails the check instead of hiding a
// certificate from it.
func (ck *crlCheck) findIssuer(cert, next *x509.Certificate) (*x509.Certificate, error) {
	candidates := ck.bySubject[string(cert.RawIssuer)]
	if next != nil {
		candidates = append([]*x509.Certificate{next}, candidates...)
	}
	for i, candidate := range candidates {
		if i > 0 && candidate == next {
			continue
		}
		issued, err := ck.issuedBy(cert, candidate)
		if err != nil {
			return nil, err
		}
		if issued {
			return candidate, nil
		}
	}
	return nil, nil
}

// issuedBy reports whether issuer signed cert, spending one of the
// connection's bounded signature checks when the names match.
func (ck *crlCheck) issuedBy(cert, issuer *x509.Certificate) (bool, error) {
	if !bytes.Equal(cert.RawIssuer, issuer.RawSubject) {
		return false, nil
	}
	if err := ck.spendSignatureCheck(); err != nil {
		return false, err
	}
	return cert.CheckSignatureFrom(issuer) == nil, nil
}

// crlsSignedBy returns the CRLs that issuer signed. Only the CRLs
// that carry the issuer's name can be, and their signature is
// verified once per issuer for the connection, spending the bounded
// signature checks.
func (ck *crlCheck) crlsSignedBy(issuer *x509.Certificate) ([]*x509.RevocationList, error) {
	if crls, done := ck.crlsByIssuer[issuer]; done {
		return crls, nil
	}
	var crls []*x509.RevocationList
	for _, crl := range ck.checker.crls {
		if !bytes.Equal(crl.RawIssuer, issuer.RawSubject) {
			continue
		}
		if err := ck.spendSignatureCheck(); err != nil {
			return nil, err
		}
		if crl.CheckSignatureFrom(issuer) == nil {
			crls = append(crls, crl)
		}
	}
	ck.crlsByIssuer[issuer] = crls
	return crls, nil
}

func (ck *crlCheck) spendSignatureCheck() error {
	if ck.signatureChecks >= maxSignatureChecks {
		return errSignatureChecksSpent
	}
	ck.signatureChecks++
	return nil
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

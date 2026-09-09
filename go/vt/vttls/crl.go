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
		// crlIssuers holds the issuer name of each CRL, to tell
		// when a certificate's issuer has a CRL that the check must
		// be able to bind. Names are compared by value, as rendered
		// by pkix.Name.String, rather than by their DER encoding: a
		// CRL made by another tool than the CA certificate can
		// encode the same name differently, and nothing rides on
		// the name alone since the CRL's signature is verified.
		crlIssuers    map[string]struct{}
		crlIssuerName map[*x509.RevocationList]string
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
		crlsByIssuer    map[string]crlBinding
		signatureChecks int
	}

	// crlBinding is what an issuer certificate makes of the CRLs
	// that carry its name: the ones it validates, and whether one
	// of them is signed by its key while it is not allowed to sign
	// CRLs, which is what a forged issuer arranges.
	crlBinding struct {
		crls     []*x509.RevocationList
		orphaned bool
	}

	// crlLookup is the outcome of looking for the issuer of a
	// certificate and for the CRLs that apply to it.
	crlLookup struct {
		issued   bool
		crls     []*x509.RevocationList
		orphaned bool
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
	checker := &crlChecker{
		crls:          crls,
		anchors:       map[string]struct{}{},
		crlIssuers:    map[string]struct{}{},
		crlIssuerName: map[*x509.RevocationList]string{},
	}
	if ca != "" {
		checker.issuers, err = loadx509Certificates(ca)
		if err != nil {
			return nil, err
		}
	}
	for _, issuer := range checker.issuers {
		checker.anchors[string(issuer.Raw)] = struct{}{}
	}
	for _, crl := range crls {
		name := crl.Issuer.String()
		checker.crlIssuers[name] = struct{}{}
		checker.crlIssuerName[crl] = name
	}
	return checker, nil
}

// hasCRLFrom reports whether a CRL carries the name of cert's issuer.
func (c *crlChecker) hasCRLFrom(cert *x509.Certificate) bool {
	_, named := c.crlIssuers[cert.Issuer.String()]
	return named
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
		crlsByIssuer: map[string]crlBinding{},
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
// leaf certificate has to be found when a CRL is configured under
// its name, so that a peer cannot dodge that CRL by leaving its chain
// out; other certificates whose issuer is not available go unchecked,
// as they always did, and so does a leaf whose issuer has no CRL
// configured, since there would be nothing to check it against. When
// a configured CRL is signed by the key of a certificate's issuer,
// one of the issuer certificates found has to be allowed to validate
// it, since a peer could otherwise present a forged issuer that
// carries the real issuer's key but not the CRL signing key usage.
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
			lookup, err := ck.crlsFor(cert)
			if err != nil {
				return fmt.Errorf("cannot check the revocation of certificate CommonName=%v: %w", cert.Subject.CommonName, err)
			}
			if !lookup.issued {
				if ck.checker.hasCRLFrom(cert) && bytes.Equal(cert.Raw, ck.presented[0].Raw) {
					return fmt.Errorf("cannot check the revocation of certificate CommonName=%v: no certificate is available for its issuer %v", cert.Subject.CommonName, cert.Issuer.CommonName)
				}
				continue
			}
			if lookup.orphaned {
				return fmt.Errorf("cannot check the revocation of certificate CommonName=%v: a CRL signed by the key of its issuer %v is configured, but none of the certificates found for that issuer may sign CRLs", cert.Subject.CommonName, cert.Issuer.CommonName)
			}
			for _, crl := range lookup.crls {
				if certIsRevoked(cert, crl) {
					return fmt.Errorf("Certificate revoked: CommonName=%v", cert.Subject.CommonName)
				}
			}
		}
	}
	return nil
}

// index records cert as a possible issuer of the certificates that
// carry its subject as their issuer, once per distinct certificate,
// keeping the order in which it was indexed: the configured issuers
// come first, then the certificates of the chains being checked.
func (ck *crlCheck) index(cert *x509.Certificate) {
	if _, done := ck.indexed[string(cert.Raw)]; done {
		return
	}
	ck.indexed[string(cert.Raw)] = struct{}{}
	ck.bySubject[string(cert.RawSubject)] = append(ck.bySubject[string(cert.RawSubject)], cert)
}

// crlsFor looks for a certificate that issued cert among the
// candidates that carry its issuer's name, and for the CRLs that such
// a certificate validates. The candidates are tried in order, the
// configured ones first, and the search stops at the first issuer
// that validates a CRL: every certificate that issued cert holds the
// same key, so the ones after it validate no other CRL. It carries
// on past an issuer that validates none, since that may be a forged
// one, and reports whether one of them left a CRL of its key's
// unvalidated. It returns an error, and not merely no issuer, when
// the connection's signature checks run out, so that a padded chain
// fails the check instead of hiding a certificate from it.
func (ck *crlCheck) crlsFor(cert *x509.Certificate) (crlLookup, error) {
	var lookup crlLookup
	if !ck.checker.hasCRLFrom(cert) {
		// Nothing could apply to cert, so its issuer is not worth
		// a signature check: not finding one changes nothing for
		// a certificate whose issuer has no CRL.
		return lookup, nil
	}
	for _, candidate := range ck.bySubject[string(cert.RawIssuer)] {
		issued, err := ck.issuedBy(cert, candidate)
		if err != nil {
			return crlLookup{}, err
		}
		if !issued {
			continue
		}
		lookup.issued = true
		binding, err := ck.crlsSignedBy(candidate)
		if err != nil {
			return crlLookup{}, err
		}
		if len(binding.crls) > 0 {
			return crlLookup{issued: true, crls: binding.crls}, nil
		}
		lookup.orphaned = lookup.orphaned || binding.orphaned
	}
	return lookup, nil
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

// crlsSignedBy returns what issuer makes of the CRLs that carry its
// name, the only ones it can have signed, verifying their signature
// once per issuer for the connection within the bounded signature
// checks. A CRL that another key signed, as happens when two CAs
// share a subject, is simply not the issuer's. A CRL that the
// issuer's own key signed while the certificate is not allowed to
// sign CRLs is reported as orphaned.
func (ck *crlCheck) crlsSignedBy(issuer *x509.Certificate) (crlBinding, error) {
	if binding, done := ck.crlsByIssuer[string(issuer.Raw)]; done {
		return binding, nil
	}
	var binding crlBinding
	issuerName := issuer.Subject.String()
	for _, crl := range ck.checker.crls {
		if ck.checker.crlIssuerName[crl] != issuerName {
			continue
		}
		if err := ck.spendSignatureCheck(); err != nil {
			return crlBinding{}, err
		}
		err := crl.CheckSignatureFrom(issuer)
		if err == nil {
			binding.crls = append(binding.crls, crl)
			continue
		}
		// CheckSignatureFrom returns the violation as is, unwrapped.
		if _, violation := err.(x509.ConstraintViolationError); !violation {
			continue
		}
		if err := ck.spendSignatureCheck(); err != nil {
			return crlBinding{}, err
		}
		if issuer.CheckSignature(crl.SignatureAlgorithm, crl.RawTBSRevocationList, crl.Signature) == nil {
			binding.orphaned = true
		}
	}
	ck.crlsByIssuer[string(issuer.Raw)] = binding
	return binding, nil
}

// spendSignatureCheck accounts for one signature verification against
// the connection's budget, before an issuer lookup or a CRL binding
// performs it, and fails once the budget is spent so that the check
// aborts rather than carrying on without the verification.
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

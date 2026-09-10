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
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/asn1"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// crlChecker rejects a connection when a certificate of the
	// peer's verified chain is listed in a configured Certificate
	// Revocation List that the certificate's issuer, the next
	// certificate of the chain, signed. Like the Go projects that
	// take a local CRL, grpc-go's advancedtls among them, it checks
	// verified chains alone: every certificate of one has an issuer
	// that verification vouches for.
	crlChecker struct {
		// crlsByIssuer holds the CRLs by their issuer name, as
		// encoded: the CRLs of an issuer certificate are the ones
		// that carry its subject, matched byte for byte, the way
		// Go matches the names of a chain when it builds one.
		crlsByIssuer map[string][]*x509.RevocationList
		// configured holds, by DER encoding, the CRLs that each
		// configured CA certificate validates, bound once here
		// rather than on every connection.
		configured map[string][]*x509.RevocationList
		// revokedAnchors holds, by DER encoding, the configured CA
		// certificates that the CRL of their issuer, another
		// configured CA certificate, lists: no chain may end at
		// one. The anchor of a chain is otherwise not checked, its
		// issuer being beyond the chain, but here the checker holds
		// both certificates and the CRL, with no part for the peer
		// in it, worked out once when the checker is built.
		revokedAnchors map[string]bool
		// bound holds, by DER encoding, what binding the CRLs to an
		// issuer that verification alone vouches for made of them,
		// as a crlBinding, kept from the first connection through
		// that issuer for the ones after it: neither the CRLs nor
		// the certificate change. Only the issuers of verified
		// chains get in, which the trusted CAs issued, and at most
		// maxBoundIssuers of them, counted by boundIssuers.
		bound        sync.Map
		boundIssuers atomic.Int64
		// revokedSerials indexes the serial numbers each CRL
		// revokes, so that a handshake looks a certificate up
		// rather than scanning a CRL that may hold many entries.
		revokedSerials map[*x509.RevocationList]map[string]struct{}
		// warningKeys holds each CRL's key for the throttle of the
		// warning about its expiry, a digest of the CRL worked out
		// once rather than on every handshake that consults it.
		warningKeys map[*x509.RevocationList]string
	}

	// crlBinding is what binding the CRLs to an issuer made of
	// them: the CRLs it validates, or why they cannot be applied.
	crlBinding struct {
		crls []*x509.RevocationList
		err  error
	}

	// issuingDistributionPoint is the extension of that name, RFC
	// 5280 section 5.2.5, by which a CRL is limited to part of what
	// its issuer revoked, or made an indirect one.
	issuingDistributionPoint struct {
		DistributionPoint          asn1.RawValue  `asn1:"tag:0,optional"`
		OnlyContainsUserCerts      bool           `asn1:"tag:1,optional"`
		OnlyContainsCACerts        bool           `asn1:"tag:2,optional"`
		OnlySomeReasons            asn1.BitString `asn1:"tag:3,optional"`
		IndirectCRL                bool           `asn1:"tag:4,optional"`
		OnlyContainsAttributeCerts bool           `asn1:"tag:5,optional"`
	}
)

// expiredCRLWarnings holds, per CRL, when the CRL was last warned
// about being past its due date, since the warning would otherwise
// repeat on every handshake that consults the CRL. One entry per CRL
// keeps every stale CRL visible in the logs.
var expiredCRLWarnings sync.Map

const expiredCRLWarningInterval = time.Minute

// maxBoundIssuers bounds how many issuers found in verified chains
// have their bindings kept, see crlChecker.bound: a peer that holds
// the key of a permitted intermediate can present a CA certificate
// minted under it on every connection, and the ones past the bound
// are bound again on each connection rather than kept.
const maxBoundIssuers = 1024

// crlClockSkew is how far in the future a CRL's thisUpdate may lie
// and still count as current, to allow for the clocks of the CA and
// of this host to disagree a little.
const crlClockSkew = 5 * time.Minute

var (
	// oidDeltaCRLIndicator is the id of the extension that marks a
	// delta CRL, RFC 5280 section 5.2.4.
	oidDeltaCRLIndicator = asn1.ObjectIdentifier{2, 5, 29, 27}
	// oidIssuingDistributionPoint is the id of the extension that
	// scopes a CRL, RFC 5280 section 5.2.5, and marks an indirect one.
	oidIssuingDistributionPoint = asn1.ObjectIdentifier{2, 5, 29, 28}
)

// unsupportedCRL reports why crl cannot be applied the way the checker
// applies every CRL, as the complete list of what its issuer revoked,
// validated by the issuer's signature, which are the CRLs that grpc-go
// takes as well: a CRL issued in the future is not current; one signed
// with an algorithm Go does not know can never be validated; a delta
// CRL's entries only make sense together with the base CRL they
// amend; an indirect CRL's entries may belong to other issuers than
// the CRL's; a CRL limited to some of the issuer's certificates, or
// to some revocation reasons, is not the complete list; and any other
// critical extension, of the CRL or of an entry, carries a meaning the
// checker does not handle, which RFC 5280 says must not be ignored.
func unsupportedCRL(crl *x509.RevocationList) error {
	if crl.ThisUpdate.After(time.Now().Add(crlClockSkew)) {
		// A CRL staged ahead of time must not supersede the current
		// one, nor be applied before its time.
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "the CRL from issuer %s is not valid yet: it was issued at %s", crl.Issuer.CommonName, crl.ThisUpdate.UTC().Format(time.RFC3339))
	}
	if crl.SignatureAlgorithm == x509.UnknownSignatureAlgorithm {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "the CRL from issuer %s cannot be validated: it is signed with an algorithm that is not supported", crl.Issuer.CommonName)
	}
	for _, extension := range crl.Extensions {
		switch {
		case extension.Id.Equal(oidDeltaCRLIndicator):
			return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "delta CRLs are not supported: the CRL from issuer %s is one", crl.Issuer.CommonName)
		case extension.Id.Equal(oidIssuingDistributionPoint):
			if err := unsupportedScope(extension.Value); err != nil {
				return vterrors.Wrapf(err, "the issuing distribution point of the CRL from issuer %s is not supported", crl.Issuer.CommonName)
			}
		case extension.Critical:
			return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "the CRL from issuer %s carries the critical extension %s, which is not supported", crl.Issuer.CommonName, extension.Id)
		}
	}
	for _, entry := range crl.RevokedCertificateEntries {
		for _, extension := range entry.Extensions {
			if extension.Critical {
				return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "the entry for serial number %s of the CRL from issuer %s carries the critical extension %s, which is not supported", entry.SerialNumber, crl.Issuer.CommonName, extension.Id)
			}
		}
	}
	return nil
}

// unsupportedScope reports why the issuing distribution point encoded
// in value makes a CRL one the checker cannot apply, see
// unsupportedCRL. A distribution point name alone is fine: a CRL
// partitioned by distribution point is still the complete list for
// the certificates of its partition, and every partition is applied.
func unsupportedScope(value []byte) error {
	var scope issuingDistributionPoint
	rest, err := asn1.Unmarshal(value, &scope)
	if err == nil && len(rest) > 0 {
		err = fmt.Errorf("%d bytes of trailing data", len(rest))
	}
	switch {
	case err != nil:
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "it cannot be parsed: %v", err)
	case scope.IndirectCRL:
		return vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "indirect CRLs are not supported")
	case scope.OnlyContainsAttributeCerts:
		return vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "the CRL is limited to attribute certificates")
	case scope.OnlyContainsUserCerts:
		return vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "the CRL is limited to end-entity certificates, and only complete CRLs are supported")
	case scope.OnlyContainsCACerts:
		return vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "the CRL is limited to CA certificates, and only complete CRLs are supported")
	case scope.OnlySomeReasons.BitLength > 0:
		return vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "the CRL is limited to some revocation reasons, and only complete CRLs are supported")
	}
	return nil
}

// expiredCRLKey identifies a CRL across the configurations that load
// it, for the throttle of the warning about its expiry, by a digest of
// its encoding: an issuer can publish several CRLs due at the same
// time, and the number that would tell them apart is optional.
func expiredCRLKey(crl *x509.RevocationList) string {
	digest := sha256.Sum256(crl.Raw)
	return hex.EncodeToString(digest[:])
}

// crlNumber renders the number of crl, which the extension carrying
// it may leave out.
func crlNumber(crl *x509.RevocationList) string {
	if crl.Number == nil {
		return ""
	}
	return crl.Number.String()
}

// warnExpiredCRL logs that crl, identified by key for the throttle,
// is past its due date, at most once per interval for that CRL,
// however many handshakes consult it at once: the one that records
// the warning first is the one that logs it.
func warnExpiredCRL(crl *x509.RevocationList, key string) {
	now := time.Now()
	if last, warned := expiredCRLWarnings.LoadOrStore(key, now); warned {
		if now.Sub(last.(time.Time)) < expiredCRLWarningInterval || !expiredCRLWarnings.CompareAndSwap(key, last, now) {
			return
		}
	}
	log.Warn("The Certificate Revocation List (CRL) is past its due date and must be updated. Revoked certificates will still be rejected in this state.",
		slog.String("issuer", crl.Issuer.CommonName),
		slog.String("crl_number", crlNumber(crl)),
		slog.Time("next_update", crl.NextUpdate),
	)
}

// isRevoked reports whether crl, which has to be one of the checker's,
// lists cert, warning when the CRL is past its due date. A CRL
// without a nextUpdate, which RFC 5280 has issuers include but
// leaves optional, has no due date to be past.
func (c *crlChecker) isRevoked(cert *x509.Certificate, crl *x509.RevocationList) bool {
	if !crl.NextUpdate.IsZero() && !time.Now().Before(crl.NextUpdate) {
		warnExpiredCRL(crl, c.warningKeys[crl])
	}
	_, revoked := c.revokedSerials[crl][cert.SerialNumber.String()]
	return revoked
}

// newCRLChecker loads the CRLs in crl and, when ca is set, the CA
// certificates that the CRLs may be signed by.
func newCRLChecker(crl, ca string) (*crlChecker, error) {
	crls, err := loadCRLSet(crl)
	if err != nil {
		return nil, err
	}
	var issuers []*x509.Certificate
	if ca != "" {
		issuers, err = loadx509Certificates(ca)
		if err != nil {
			return nil, err
		}
	}
	return newCRLCheckerFrom(crls, issuers)
}

// newCRLCheckerFrom builds the checker of the given CRLs and configured
// CA certificates, indexing the CRLs and binding them to the
// certificates once. A CRL that a configured certificate does not
// validate while it ought to, see bindCRLs, is refused here, at
// startup, rather than on every connection under that certificate.
func newCRLCheckerFrom(crls []*x509.RevocationList, issuers []*x509.Certificate) (*crlChecker, error) {
	checker := &crlChecker{
		crlsByIssuer:   make(map[string][]*x509.RevocationList),
		configured:     make(map[string][]*x509.RevocationList, len(issuers)),
		revokedAnchors: make(map[string]bool),
		revokedSerials: make(map[*x509.RevocationList]map[string]struct{}, len(crls)),
		warningKeys:    make(map[*x509.RevocationList]string, len(crls)),
	}
	for _, crl := range crls {
		checker.crlsByIssuer[string(crl.RawIssuer)] = append(checker.crlsByIssuer[string(crl.RawIssuer)], crl)
		serials := make(map[string]struct{}, len(crl.RevokedCertificateEntries))
		for _, revoked := range crl.RevokedCertificateEntries {
			serials[revoked.SerialNumber.String()] = struct{}{}
		}
		checker.revokedSerials[crl] = serials
		checker.warningKeys[crl] = expiredCRLKey(crl)
	}
	applied := make(map[*x509.RevocationList]bool, len(crls))
	for _, issuer := range issuers {
		if _, done := checker.configured[string(issuer.Raw)]; done {
			continue
		}
		issuerCRLs, err := checker.bindCRLs(issuer)
		if err != nil {
			return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "the CRLs cannot be applied under the configured CA certificate %s: %v", issuer.Subject.CommonName, err)
		}
		checker.configured[string(issuer.Raw)] = issuerCRLs
		for _, crl := range issuerCRLs {
			applied[crl] = true
		}
	}
	// A CRL that no configured certificate applies is right for the
	// CRL of a CA that peers present, but silence for a configured
	// CA's whose issuer name is encoded differently from the
	// certificate's subject, as a CRL that another tool than the
	// CA's wrote can be: the names are matched byte for byte, so
	// nothing would read it. Such a CRL is told by its signature,
	// once here, and refused rather than left unapplied.
	for _, crl := range crls {
		if applied[crl] {
			continue
		}
		for _, issuer := range issuers {
			if !bytes.Equal(crl.RawIssuer, issuer.RawSubject) && crl.CheckSignatureFrom(issuer) == nil {
				return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "the CRL from issuer %s is signed by the configured CA certificate %s, but its issuer name is encoded differently from that certificate's subject, so it would not be applied: re-issue the CRL with the certificate's subject as its issuer", crl.Issuer.CommonName, issuer.Subject.CommonName)
			}
		}
	}
	for _, cert := range issuers {
		for _, parent := range issuers {
			if checker.revokedAnchors[string(cert.Raw)] || cert.Equal(parent) || !bytes.Equal(cert.RawIssuer, parent.RawSubject) || cert.CheckSignatureFrom(parent) != nil {
				continue
			}
			for _, crl := range checker.configured[string(parent.Raw)] {
				if checker.isRevoked(cert, crl) {
					checker.revokedAnchors[string(cert.Raw)] = true
					log.Warn("A configured CA certificate is revoked by the CRL of its issuer: connections whose chain ends at it will be rejected.",
						slog.String("subject", cert.Subject.CommonName),
						slog.String("issuer", parent.Subject.CommonName),
					)
				}
			}
		}
	}
	return checker, nil
}

// bindCRLs returns the CRLs that issuer validates, the newest complete
// one for each scope, among the CRLs that carry its subject as their
// issuer. A CRL is bound by its signature, which the issuer has to be
// allowed to make, with the cRLSign key usage. A CRL under the
// issuer's name that it does not validate is refused rather than
// passed over, since nothing else tells a CRL of the issuer's that
// has gone bad from another CA's under the same name, and the check
// fails closed; except when the CRL's authority key identifier names
// another key than the issuer's, which is what the CRL of a re-keyed
// CA's predecessor does.
func (c *crlChecker) bindCRLs(issuer *x509.Certificate) ([]*x509.RevocationList, error) {
	var crls []*x509.RevocationList
	for _, crl := range c.crlsByIssuer[string(issuer.RawSubject)] {
		err := crl.CheckSignatureFrom(issuer)
		if err == nil {
			crls = append(crls, crl)
			continue
		}
		if len(crl.AuthorityKeyId) > 0 && len(issuer.SubjectKeyId) > 0 && !bytes.Equal(crl.AuthorityKeyId, issuer.SubjectKeyId) {
			continue
		}
		// CheckSignatureFrom returns the violation as is, unwrapped.
		if _, violation := err.(x509.ConstraintViolationError); violation {
			return nil, fmt.Errorf("the CRL from issuer %s cannot be validated: the certificate of that issuer is not allowed to sign CRLs", crl.Issuer.CommonName)
		}
		return nil, fmt.Errorf("the CRL from issuer %s cannot be validated: its signature does not verify against the certificate of that issuer: %w", crl.Issuer.CommonName, err)
	}
	return newestCompleteCRLs(crls), nil
}

// newestCompleteCRLs keeps, of several complete CRLs that one issuer
// certificate validated, the newest for each scope alone: a complete
// CRL supersedes the ones issued before it, and an entry of an older
// one that the newest dropped, such as a certificate taken off hold,
// is a revocation no more. Supersession is decided among the CRLs
// that one certificate validated, so that the CRLs of two CAs sharing
// a name, as a re-keyed CA and its predecessor do, never supersede
// each other. The CRLs keep their order otherwise.
func newestCompleteCRLs(crls []*x509.RevocationList) []*x509.RevocationList {
	newest := make(map[string]*x509.RevocationList, len(crls))
	for _, crl := range crls {
		if current, found := newest[crlScope(crl)]; !found || newerCRL(crl, current) {
			newest[crlScope(crl)] = crl
		}
	}
	kept := make([]*x509.RevocationList, 0, len(newest))
	for _, crl := range crls {
		if newest[crlScope(crl)] == crl {
			kept = append(kept, crl)
		}
	}
	return kept
}

// crlScope renders the issuing distribution point of crl, which tells
// the partitioned CRLs of one issuer apart, or nothing for a CRL that
// has none.
func crlScope(crl *x509.RevocationList) string {
	for _, extension := range crl.Extensions {
		if extension.Id.Equal(oidIssuingDistributionPoint) {
			return hex.EncodeToString(extension.Value)
		}
	}
	return ""
}

// newerCRL reports whether a is a newer CRL than b: by number when
// both carry one and they differ, by issue time otherwise.
func newerCRL(a, b *x509.RevocationList) bool {
	if a.Number != nil && b.Number != nil && a.Number.Cmp(b.Number) != 0 {
		return a.Number.Cmp(b.Number) > 0
	}
	return a.ThisUpdate.After(b.ThisUpdate)
}

// verifyConnection is a tls.Config.VerifyConnection callback for the
// configurations in which Go verifies the peer itself: the server
// side and the verify_identity client mode. Unlike
// VerifyPeerCertificate, Go runs it on every connection: it is not
// skipped on resumed sessions, whose state carries the chains that
// were verified when the session was established.
func (c *crlChecker) verifyConnection(cs tls.ConnectionState) error {
	return c.check(cs.VerifiedChains)
}

// check rejects the connection when a certificate of the peer's
// verified chains is revoked. The peer passes when any chain passes,
// as verification itself accepts the peer on the strength of any
// valid chain: an intermediate cross-signed by two roots may be
// revoked by one and not the other, as during a CA rollover, and the
// chain through the other root still carries the peer. When every
// chain fails, the peer fails with the first chain's failure.
func (c *crlChecker) check(chains [][]*x509.Certificate) error {
	var failure error
	for _, chain := range chains {
		err := c.checkChain(chain)
		if err == nil {
			return nil
		}
		if failure == nil {
			failure = err
		}
	}
	return failure
}

// checkChain checks every certificate of a verified chain but the
// trust anchor it ends at against the CRLs of its issuer, the next
// certificate of the chain, which verification vouches signed it. The
// anchor is trusted as configured: its issuer is not part of the
// chain, and Go does not verify its signature either; only an anchor
// that its issuer's CRL is known to list, see revokedAnchors, ends
// no chain. The chain is walked from the anchor down, so that a
// revoked CA certificate is found before the certificates below it
// are bound: whoever holds its key can mint any number of those, and
// none is worth keeping.
func (c *crlChecker) checkChain(chain []*x509.Certificate) error {
	if len(chain) == 0 {
		return nil
	}
	if anchor := chain[len(chain)-1]; c.revokedAnchors[string(anchor.Raw)] {
		return vterrors.Errorf(vtrpc.Code_UNAUTHENTICATED, "Certificate revoked: CommonName=%s", anchor.Subject.CommonName)
	}
	for i := len(chain) - 2; i >= 0; i-- {
		if err := c.checkCertificate(chain[i], chain[i+1]); err != nil {
			return err
		}
	}
	return nil
}

// checkCertificate checks cert against the CRLs that issuer validates.
func (c *crlChecker) checkCertificate(cert, issuer *x509.Certificate) error {
	crls, err := c.crlsOf(issuer)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_UNAUTHENTICATED, "cannot check the revocation of certificate CommonName=%s: %v", cert.Subject.CommonName, err)
	}
	for _, crl := range crls {
		if c.isRevoked(cert, crl) {
			return vterrors.Errorf(vtrpc.Code_UNAUTHENTICATED, "Certificate revoked: CommonName=%s", cert.Subject.CommonName)
		}
	}
	return nil
}

// crlsOf returns the CRLs that issuer validates, or why they cannot
// be applied: bound when the checker was built for a configured CA
// certificate, and on first sight for an issuer that verification
// alone vouches for, such as an intermediate the peer presented,
// whose CRLs cost a signature check each that one connection.
func (c *crlChecker) crlsOf(issuer *x509.Certificate) ([]*x509.RevocationList, error) {
	if crls, configured := c.configured[string(issuer.Raw)]; configured {
		return crls, nil
	}
	if cached, done := c.bound.Load(string(issuer.Raw)); done {
		binding := cached.(crlBinding)
		return binding.crls, binding.err
	}
	crls, err := c.bindCRLs(issuer)
	if c.boundIssuers.Load() < maxBoundIssuers {
		if _, loaded := c.bound.LoadOrStore(string(issuer.Raw), crlBinding{crls: crls, err: err}); !loaded {
			c.boundIssuers.Add(1)
		}
	}
	return crls, err
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
		if err := unsupportedCRL(parsedCRL); err != nil {
			return nil, vterrors.Wrapf(err, "cannot use the CRL file %s", crl)
		}
		crlSet = append(crlSet, parsedCRL)
	}
	if len(crlSet) == 0 {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "no CRL found in file: %s", crl)
	}
	return crlSet, nil
}

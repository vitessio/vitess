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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"

	"vitess.io/vitess/go/vt/log"
)

// maxSignatureChecks bounds the signature verifications spent per
// connection on finding the issuers of the peer's certificates and on
// binding the CRLs to them, since the peer controls the number and
// the names of the certificates it presents. It mirrors the bound
// that crypto/x509 puts on chain building.
const maxSignatureChecks = 100

var errSignatureChecksSpent = fmt.Errorf("checking it exceeded the %d signature checks allowed per connection", maxSignatureChecks)

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
		// crlIssuers holds the issuer name of each CRL, as
		// rendered by nameKey, to tell when a certificate's issuer
		// has a CRL that the check must be able to bind.
		crlIssuers    map[string]struct{}
		crlIssuerName map[*x509.RevocationList]string
		// configuredBindings holds, by DER encoding, what each
		// configured issuer makes of the CRLs, worked out once
		// here rather than on every connection: only the issuers
		// that a peer presents, whose number the peer controls,
		// spend a connection's signature checks on that.
		configuredBindings map[string]crlBinding
		// configuredBySubject indexes the configured issuers by
		// subject, as rendered by nameKey, once here rather than on
		// every connection, since a CA file can hold many.
		configuredBySubject map[string][]*x509.Certificate
		// revokedSerials indexes the serial numbers each CRL
		// revokes, so that a handshake looks a certificate up
		// rather than scanning a CRL that may hold many entries.
		revokedSerials map[*x509.RevocationList]map[string]struct{}
		// warningKeys holds each CRL's key for the throttle of the
		// warning about its expiry, a digest of the CRL worked out
		// once rather than on every handshake that consults it.
		warningKeys map[*x509.RevocationList]string
	}

	// crlCheck is the state of one connection's revocation check.
	crlCheck struct {
		checker   *crlChecker
		presented []*x509.Certificate
		// chains are the certificate chains the check walks, and
		// verified tells whether verification built them, in which
		// case the certificate each ends at is a trust anchor whose
		// issuer need not be available.
		chains   [][]*x509.Certificate
		verified bool
		// bySubject indexes the certificates of the connection that
		// may be an issuer by subject, as rendered by nameKey: the
		// ones from the chains being checked, then the presented
		// ones. The configured issuers are indexed by the checker.
		bySubject map[string][]*x509.Certificate
		indexed   map[string]struct{}
		// names memoizes nameKey by DER encoded name.
		names           map[string]string
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

	// rawRDNSequence is a distinguished name whose attribute values
	// are kept as they were encoded, since encoding/asn1 decodes
	// only some of the string types a DirectoryString may use and
	// leaves the others as nothing.
	rawRDNSequence []rawRelativeDistinguishedNameSET
	// rawRelativeDistinguishedNameSET is one RDN of a rawRDNSequence;
	// the SET suffix has encoding/asn1 treat it as a SET OF.
	rawRelativeDistinguishedNameSET []rawAttributeTypeAndValue
	rawAttributeTypeAndValue        struct {
		Type  asn1.ObjectIdentifier
		Value asn1.RawValue
	}

	// issuingDistributionPoint is the part of the extension of that
	// name that the checker cares about: whether the CRL is an
	// indirect one, whose entries may belong to other issuers than
	// the CRL's.
	issuingDistributionPoint struct {
		DistributionPoint          asn1.RawValue  `asn1:"tag:0,optional"`
		OnlyContainsUserCerts      bool           `asn1:"tag:1,optional"`
		OnlyContainsCACerts        bool           `asn1:"tag:2,optional"`
		OnlySomeReasons            asn1.BitString `asn1:"tag:3,optional"`
		IndirectCRL                bool           `asn1:"tag:4,optional"`
		OnlyContainsAttributeCerts bool           `asn1:"tag:5,optional"`
	}
)

// tagUniversalString is the ASN.1 tag of UniversalString, which
// encoding/asn1 has no constant for.
const tagUniversalString = 28

// directoryString decodes the string types that a DirectoryString, or
// the other string types found in names, may be encoded as. A T.61
// string is read as Latin-1, as X.509 tooling conventionally does:
// T.61 proper differs from Latin-1 beyond ASCII, but the tools that
// emitted T.61 strings put Latin-1 in them.
func directoryString(value asn1.RawValue) (string, bool) {
	if value.Class != asn1.ClassUniversal {
		return "", false
	}
	switch value.Tag {
	case asn1.TagT61String:
		runes := make([]rune, len(value.Bytes))
		for i, b := range value.Bytes {
			runes[i] = rune(b)
		}
		return string(runes), true
	case asn1.TagUTF8String, asn1.TagPrintableString, asn1.TagIA5String, asn1.TagGeneralString, asn1.TagNumericString:
		return string(value.Bytes), true
	case asn1.TagBMPString:
		if len(value.Bytes)%2 != 0 {
			return "", false
		}
		units := make([]uint16, 0, len(value.Bytes)/2)
		for i := 0; i < len(value.Bytes); i += 2 {
			units = append(units, uint16(value.Bytes[i])<<8|uint16(value.Bytes[i+1]))
		}
		return string(utf16.Decode(units)), true
	case tagUniversalString:
		if len(value.Bytes)%4 != 0 {
			return "", false
		}
		runes := make([]rune, 0, len(value.Bytes)/4)
		for i := 0; i < len(value.Bytes); i += 4 {
			runes = append(runes, rune(value.Bytes[i])<<24|rune(value.Bytes[i+1])<<16|rune(value.Bytes[i+2])<<8|rune(value.Bytes[i+3]))
		}
		return string(runes), true
	}
	return "", false
}

// expiredCRLWarnings holds, per CRL, when the CRL was last warned
// about being past its due date, since the warning would otherwise
// repeat on every handshake that consults the CRL. One entry per CRL
// keeps every stale CRL visible in the logs.
var expiredCRLWarnings sync.Map

const expiredCRLWarningInterval = time.Minute

var (
	// oidDeltaCRLIndicator is the id of the extension that marks a
	// delta CRL, RFC 5280 section 5.2.4.
	oidDeltaCRLIndicator = asn1.ObjectIdentifier{2, 5, 29, 27}
	// oidIssuingDistributionPoint is the id of the extension that
	// scopes a CRL, RFC 5280 section 5.2.5, and marks an indirect one.
	oidIssuingDistributionPoint = asn1.ObjectIdentifier{2, 5, 29, 28}
)

// unsupportedCRL reports why crl cannot be evaluated on its own, as
// the checker evaluates every CRL: a delta CRL's entries only make
// sense together with the base CRL they amend, and an indirect CRL's
// entries may belong to other issuers than the CRL's.
func unsupportedCRL(crl *x509.RevocationList) error {
	for _, extension := range crl.Extensions {
		switch {
		case extension.Id.Equal(oidDeltaCRLIndicator):
			return fmt.Errorf("delta CRLs are not supported: the CRL from issuer %s is one", crl.Issuer.CommonName)
		case extension.Id.Equal(oidIssuingDistributionPoint):
			var scope issuingDistributionPoint
			if _, err := asn1.Unmarshal(extension.Value, &scope); err != nil {
				return fmt.Errorf("the issuing distribution point of the CRL from issuer %s cannot be parsed: %w", crl.Issuer.CommonName, err)
			}
			if scope.IndirectCRL {
				return fmt.Errorf("indirect CRLs are not supported: the CRL from issuer %s is one", crl.Issuer.CommonName)
			}
		}
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
// lists cert, warning when the CRL is past its due date.
func (c *crlChecker) isRevoked(cert *x509.Certificate, crl *x509.RevocationList) bool {
	if !time.Now().Before(crl.NextUpdate) {
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
// issuers, indexing the CRLs and binding them to the issuers once.
func newCRLCheckerFrom(crls []*x509.RevocationList, issuers []*x509.Certificate) (*crlChecker, error) {
	checker := &crlChecker{
		crls:                crls,
		issuers:             issuers,
		crlIssuers:          map[string]struct{}{},
		crlIssuerName:       map[*x509.RevocationList]string{},
		configuredBindings:  map[string]crlBinding{},
		configuredBySubject: map[string][]*x509.Certificate{},
		revokedSerials:      map[*x509.RevocationList]map[string]struct{}{},
		warningKeys:         map[*x509.RevocationList]string{},
	}
	for _, crl := range crls {
		name := nameKey(crl.RawIssuer)
		checker.crlIssuers[name] = struct{}{}
		checker.crlIssuerName[crl] = name
		serials := make(map[string]struct{}, len(crl.RevokedCertificateEntries))
		for _, revoked := range crl.RevokedCertificateEntries {
			serials[revoked.SerialNumber.String()] = struct{}{}
		}
		checker.revokedSerials[crl] = serials
		checker.warningKeys[crl] = expiredCRLKey(crl)
	}
	unbounded := func() error { return nil }
	for _, issuer := range checker.issuers {
		if _, done := checker.configuredBindings[string(issuer.Raw)]; done {
			continue
		}
		subject := nameKey(issuer.RawSubject)
		binding, err := checker.bindCRLs(issuer, subject, unbounded)
		if err != nil {
			return nil, err
		}
		checker.configuredBindings[string(issuer.Raw)] = binding
		checker.configuredBySubject[subject] = append(checker.configuredBySubject[subject], issuer)
	}
	return checker, nil
}

// bindCRLs works out what issuer makes of the CRLs that carry its
// name, the only ones it can have signed, calling spend before each
// signature verification. A CRL that another key signed, as happens
// when two CAs share a subject, is simply not the issuer's. A CRL that
// the issuer's own key signed while the certificate is not allowed to
// sign CRLs is reported as orphaned.
func (c *crlChecker) bindCRLs(issuer *x509.Certificate, issuerName string, spend func() error) (crlBinding, error) {
	var binding crlBinding
	for _, crl := range c.crls {
		if c.crlIssuerName[crl] != issuerName {
			continue
		}
		if err := spend(); err != nil {
			return crlBinding{}, err
		}
		err := crl.CheckSignatureFrom(issuer)
		if err == nil {
			binding.crls = append(binding.crls, crl)
			continue
		}
		// CheckSignatureFrom returns the violation as is, unwrapped.
		if _, violation := err.(x509.ConstraintViolationError); violation {
			if err := spend(); err != nil {
				return crlBinding{}, err
			}
			if issuer.CheckSignature(crl.SignatureAlgorithm, crl.RawTBSRevocationList, crl.Signature) == nil {
				binding.orphaned = true
			}
			continue
		}
		// A CRL signed with an algorithm that Go refuses to verify
		// cannot pass for the CRL of another CA under the same name:
		// it may well be this issuer's, and it is then a CRL the
		// operator relies on that cannot be applied.
		if _, insecure := err.(x509.InsecureAlgorithmError); insecure || errors.Is(err, x509.ErrUnsupportedAlgorithm) {
			return crlBinding{}, fmt.Errorf("the CRL from issuer %s cannot be validated: %w", crl.Issuer.CommonName, err)
		}
		// Nor can a CRL that names this very certificate as its
		// authority: its signature ought to verify, and one that does
		// not is a CRL that has gone bad, not another CA's.
		if len(crl.AuthorityKeyId) > 0 && bytes.Equal(crl.AuthorityKeyId, issuer.SubjectKeyId) {
			return crlBinding{}, fmt.Errorf("the CRL from issuer %s names the certificate found for that issuer as its authority, but its signature does not verify: %w", crl.Issuer.CommonName, err)
		}
		// Signed by another key under the same name: not this issuer's.
	}
	return binding, nil
}

// hasCRLFrom reports whether a CRL carries the given issuer name, as
// rendered by nameKey.
func (c *crlChecker) hasCRLFrom(issuerName string) bool {
	_, named := c.crlIssuers[issuerName]
	return named
}

// nameKey renders a DER encoded distinguished name for comparison
// under the X.509 matching rules, by which attribute values compare
// after Unicode normalization and case folding and without regard to
// leading, trailing, and repeated whitespace, and the attributes of a
// multi-valued RDN compare as a set; the string preparation of RFC
// 4518 is approximated, not followed to the letter. Go compares the
// names of certificates byte for byte, but a CRL
// can come from another tool than the CA certificate and encode,
// case, space, or order the same name differently, and nothing rides
// on the name alone since the CRL's signature is verified before the
// CRL is applied. Every component of the key is prefixed with its
// length, so that delimiter characters inside a value cannot make
// distinct names collide. A name that does not parse is compared as
// it is.
func nameKey(rawName []byte) string {
	var sequence rawRDNSequence
	if rest, err := asn1.Unmarshal(rawName, &sequence); err != nil || len(rest) > 0 {
		return string(rawName)
	}
	var key strings.Builder
	for _, rdn := range sequence {
		attributes := make([]string, 0, len(rdn))
		for _, attribute := range rdn {
			value := hex.EncodeToString(attribute.Value.FullBytes)
			if text, ok := directoryString(attribute.Value); ok {
				value = strings.Join(strings.Fields(cases.Fold().String(norm.NFKC.String(text))), " ")
			}
			oid := attribute.Type.String()
			attributes = append(attributes, fmt.Sprintf("%d:%s%d:%s", len(oid), oid, len(value), value))
		}
		slices.Sort(attributes)
		fmt.Fprintf(&key, "%d:", len(attributes))
		for _, attribute := range attributes {
			key.WriteString(attribute)
		}
	}
	return key.String()
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
// them play no part in the trust decision, so they are not checked
// themselves and cannot be used to make the check expensive, though
// they may serve as the issuer of a certificate that is checked. Only
// when there is no verified chain, because verification is disabled,
// is the presented chain checked as it is.
func (c *crlChecker) newCheck(presented []*x509.Certificate, verifiedChains [][]*x509.Certificate) *crlCheck {
	check := &crlCheck{
		checker:      c,
		presented:    presented,
		chains:       verifiedChains,
		verified:     true,
		bySubject:    map[string][]*x509.Certificate{},
		indexed:      map[string]struct{}{},
		names:        map[string]string{},
		crlsByIssuer: map[string]crlBinding{},
	}
	if len(verifiedChains) == 0 {
		check.chains = [][]*x509.Certificate{presented}
		check.verified = false
	}
	for _, chain := range check.chains {
		for _, cert := range chain {
			check.index(cert)
		}
	}
	// The certificates presented beyond the chains being checked are
	// not checked, but one of them may be the issuer of a certificate
	// that is, such as a root presented beyond the intermediate that
	// a verified chain ends at. Serving as a candidate costs nothing
	// unless a checked certificate carries the candidate's name.
	for _, cert := range presented {
		check.index(cert)
	}
	return check
}

// run walks the chains in the order the certificates were sent, and
// checks every certificate in them, the one a verified chain ends at
// included: a configured CA that verification stops at can still be
// revoked by the CRL of its own issuer when that issuer is configured
// or presented too. A self-signed certificate is checked like any
// other, so that recognizing it costs one of the bounded signature
// checks like any other issuer lookup. A certificate whose issuer
// cannot be found while a CRL is configured under its issuer's name
// fails the check, so that a peer cannot dodge that CRL by leaving
// part of its chain out, unless it is the trust anchor a verified
// chain ends at: verification vouches for that one, and its issuer,
// when neither configured nor presented, is not the peer's to supply.
// A certificate whose issuer has no CRL configured goes unchecked,
// since there would be nothing to check it against. When
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
			lookup, err := ck.crlsFor(cert)
			if err != nil {
				return fmt.Errorf("cannot check the revocation of certificate CommonName=%s: %w", cert.Subject.CommonName, err)
			}
			if !lookup.issued {
				anchor := ck.verified && i == len(chain)-1
				if !anchor && ck.checker.hasCRLFrom(ck.nameOf(cert.RawIssuer)) {
					return fmt.Errorf("cannot check the revocation of certificate CommonName=%s: no certificate is available for its issuer %s", cert.Subject.CommonName, cert.Issuer.CommonName)
				}
				continue
			}
			if lookup.orphaned {
				return fmt.Errorf("cannot check the revocation of certificate CommonName=%s: a CRL signed by the key of its issuer %s is configured, but none of the certificates found for that issuer may sign CRLs", cert.Subject.CommonName, cert.Issuer.CommonName)
			}
			for _, crl := range lookup.crls {
				if ck.checker.isRevoked(cert, crl) {
					return fmt.Errorf("Certificate revoked: CommonName=%s", cert.Subject.CommonName)
				}
			}
		}
	}
	return nil
}

// index records cert as a possible issuer of the certificates that
// carry its subject as their issuer, once per distinct certificate
// and leaving out the configured issuers, which the checker indexed,
// keeping the order in which it was indexed: the certificates of the
// chains being checked come first, then the presented ones.
func (ck *crlCheck) index(cert *x509.Certificate) {
	if _, configured := ck.checker.configuredBindings[string(cert.Raw)]; configured {
		return
	}
	if _, done := ck.indexed[string(cert.Raw)]; done {
		return
	}
	ck.indexed[string(cert.Raw)] = struct{}{}
	subject := ck.nameOf(cert.RawSubject)
	ck.bySubject[subject] = append(ck.bySubject[subject], cert)
}

// nameOf renders a DER encoded name with nameKey, once per name for
// the connection.
func (ck *crlCheck) nameOf(rawName []byte) string {
	if name, done := ck.names[string(rawName)]; done {
		return name
	}
	name := nameKey(rawName)
	ck.names[string(rawName)] = name
	return name
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
	issuerName := ck.nameOf(cert.RawIssuer)
	if !ck.checker.hasCRLFrom(issuerName) {
		// Nothing could apply to cert, so its issuer is not worth
		// a signature check: not finding one changes nothing for
		// a certificate whose issuer has no CRL.
		return lookup, nil
	}
	for _, candidate := range slices.Concat(ck.checker.configuredBySubject[issuerName], ck.bySubject[issuerName]) {
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
// connection's bounded signature checks when the names match under
// the X.509 rules. Go itself matches the names of a chain byte for
// byte, but the chains a peer presents in the non-verifying modes
// never go through Go's verifier, and the signature is what binds.
func (ck *crlCheck) issuedBy(cert, issuer *x509.Certificate) (bool, error) {
	if ck.nameOf(cert.RawIssuer) != ck.nameOf(issuer.RawSubject) {
		return false, nil
	}
	if err := ck.spendSignatureCheck(); err != nil {
		return false, err
	}
	return cert.CheckSignatureFrom(issuer) == nil, nil
}

// crlsSignedBy returns what issuer makes of the CRLs that carry its
// name: worked out once for a configured issuer when the checker was
// built, and once per connection for an issuer the peer presented,
// within the bounded signature checks.
func (ck *crlCheck) crlsSignedBy(issuer *x509.Certificate) (crlBinding, error) {
	if binding, configured := ck.checker.configuredBindings[string(issuer.Raw)]; configured {
		return binding, nil
	}
	if binding, done := ck.crlsByIssuer[string(issuer.Raw)]; done {
		return binding, nil
	}
	binding, err := ck.checker.bindCRLs(issuer, ck.nameOf(issuer.RawSubject), ck.spendSignatureCheck)
	if err != nil {
		return crlBinding{}, err
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
		if err := unsupportedCRL(parsedCRL); err != nil {
			return nil, fmt.Errorf("%w (file %s)", err, crl)
		}
		crlSet = append(crlSet, parsedCRL)
	}
	if len(crlSet) == 0 {
		return nil, fmt.Errorf("no CRL found in file: %s", crl)
	}
	return crlSet, nil
}

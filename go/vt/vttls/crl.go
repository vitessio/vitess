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
		// crlsByIssuerName indexes the CRLs by their issuer name, as
		// rendered by nameKey, both to tell when a certificate's
		// issuer has a CRL that the check must be able to bind and
		// to bind an issuer to its CRLs without scanning them all.
		crlsByIssuerName map[string][]*x509.RevocationList
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
		// scopes holds, for each CRL whose issuing distribution
		// point limits it to end-entity or to CA certificates, that
		// limit, which a CRL without one does not have.
		scopes map[*x509.RevocationList]issuingDistributionPoint
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
		// trusted indexes, by subject as rendered by nameKey, the
		// certificates of the verified chains, which verification
		// vouches for, and bySubject the other certificates the
		// peer presented, which it alone vouches for. The configured
		// issuers are indexed by the checker.
		trusted   map[string][]*x509.Certificate
		bySubject map[string][]*x509.Certificate
		indexed   map[string]struct{}
		// presentedIndexed tells whether the presented certificates
		// have been indexed into bySubject, which is put off until a
		// checked certificate's issuer has to be looked for among
		// them, since a peer can present many that nothing needs.
		presentedIndexed bool
		// names memoizes nameKey by DER encoded name.
		names           map[string]renderedName
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

	// renderedName is what nameKey makes of a name.
	renderedName struct {
		key string
		err error
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

// errUndecodableName is returned for a name that holds a T.61 string
// with characters beyond ASCII. Beyond ASCII, the bytes of a T.61
// string are ambiguous in practice: T.61 proper spells characters
// with diacritic prefixes and its own letters, while the tools that
// emitted such strings often put Latin-1 in them instead, and either
// reading misses the other, so such a name cannot be compared with
// its other encodings.
var errUndecodableName = errors.New("the name holds a T.61 string with characters beyond ASCII, which cannot be compared with other encodings of the name")

// directoryString decodes the string types that a DirectoryString, or
// the other string types found in names, may be encoded as. A T.61
// string is decoded within ASCII, where every reading of it agrees,
// and left as it is beyond that, see errUndecodableName.
func directoryString(value asn1.RawValue) (string, bool) {
	if value.Class != asn1.ClassUniversal {
		return "", false
	}
	switch value.Tag {
	case asn1.TagT61String:
		if !isASCII(value.Bytes) {
			return "", false
		}
		return string(value.Bytes), true
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

func isASCII(text []byte) bool {
	for _, b := range text {
		if b >= 0x80 {
			return false
		}
	}
	return true
}

// expiredCRLWarnings holds, per CRL, when the CRL was last warned
// about being past its due date, since the warning would otherwise
// repeat on every handshake that consults the CRL. One entry per CRL
// keeps every stale CRL visible in the logs.
var expiredCRLWarnings sync.Map

const expiredCRLWarningInterval = time.Minute

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

// unsupportedCRL reports why crl cannot be evaluated the way the
// checker evaluates every CRL, on its own and against the public key
// certificates its issuer signed: a CRL issued in the future is not
// current, a delta CRL's entries only make
// sense together with the base CRL they amend, an indirect CRL's
// entries may belong to other issuers than the CRL's, a CRL scoped to
// attribute certificates covers no certificate the checker sees, and
// any other critical extension, of the CRL or of an entry, carries a
// meaning the checker does not handle, which RFC 5280 says must not
// be ignored.
func unsupportedCRL(crl *x509.RevocationList) error {
	if crl.ThisUpdate.After(time.Now().Add(crlClockSkew)) {
		// A CRL staged ahead of time must not supersede the current
		// one, nor be applied before its time.
		return fmt.Errorf("the CRL from issuer %s is not valid yet: it was issued at %s", crl.Issuer.CommonName, crl.ThisUpdate.UTC().Format(time.RFC3339))
	}
	for _, extension := range crl.Extensions {
		switch {
		case extension.Id.Equal(oidDeltaCRLIndicator):
			return fmt.Errorf("delta CRLs are not supported: the CRL from issuer %s is one", crl.Issuer.CommonName)
		case extension.Id.Equal(oidIssuingDistributionPoint):
			var scope issuingDistributionPoint
			rest, err := asn1.Unmarshal(extension.Value, &scope)
			if err == nil && len(rest) > 0 {
				err = fmt.Errorf("%d bytes of trailing data", len(rest))
			}
			if err != nil {
				return fmt.Errorf("the issuing distribution point of the CRL from issuer %s cannot be parsed: %w", crl.Issuer.CommonName, err)
			}
			if scope.IndirectCRL {
				return fmt.Errorf("indirect CRLs are not supported: the CRL from issuer %s is one", crl.Issuer.CommonName)
			}
			if scope.OnlyContainsAttributeCerts {
				return fmt.Errorf("the CRL from issuer %s covers attribute certificates only", crl.Issuer.CommonName)
			}
		case extension.Critical:
			return fmt.Errorf("the CRL from issuer %s carries the critical extension %s, which is not supported", crl.Issuer.CommonName, extension.Id)
		}
	}
	for _, entry := range crl.RevokedCertificateEntries {
		for _, extension := range entry.Extensions {
			if extension.Critical {
				return fmt.Errorf("the entry for serial number %s of the CRL from issuer %s carries the critical extension %s, which is not supported", entry.SerialNumber, crl.Issuer.CommonName, extension.Id)
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
		crlsByIssuerName:    map[string][]*x509.RevocationList{},
		configuredBindings:  map[string]crlBinding{},
		configuredBySubject: map[string][]*x509.Certificate{},
		revokedSerials:      map[*x509.RevocationList]map[string]struct{}{},
		warningKeys:         map[*x509.RevocationList]string{},
		scopes:              map[*x509.RevocationList]issuingDistributionPoint{},
	}
	for _, issuer := range issuers {
		if _, err := nameKey(issuer.RawSubject); err != nil {
			return nil, fmt.Errorf("the configured CA certificate %s cannot be matched with the CRLs: %w", issuer.Subject.CommonName, err)
		}
	}
	for _, crl := range crls {
		name, err := nameKey(crl.RawIssuer)
		if err != nil {
			return nil, fmt.Errorf("the CRL from issuer %s cannot be matched with its issuer: %w", crl.Issuer.CommonName, err)
		}
		checker.crlsByIssuerName[name] = append(checker.crlsByIssuerName[name], crl)
		serials := make(map[string]struct{}, len(crl.RevokedCertificateEntries))
		for _, revoked := range crl.RevokedCertificateEntries {
			serials[revoked.SerialNumber.String()] = struct{}{}
		}
		checker.revokedSerials[crl] = serials
		checker.warningKeys[crl] = expiredCRLKey(crl)
		if scope, scoped := issuingScope(crl); scoped {
			checker.scopes[crl] = scope
		}
	}
	unbounded := func() error { return nil }
	for _, issuer := range checker.issuers {
		if _, done := checker.configuredBindings[string(issuer.Raw)]; done {
			continue
		}
		subject, _ := nameKey(issuer.RawSubject)
		binding, err := checker.bindCRLs(issuer, subject, unbounded)
		if err != nil {
			return nil, err
		}
		checker.configuredBindings[string(issuer.Raw)] = binding
		checker.configuredBySubject[subject] = append(checker.configuredBySubject[subject], issuer)
	}
	return checker, nil
}

// newestCompleteCRLs keeps, of several complete CRLs that one issuer
// certificate validated, the newest for each scope alone: a complete
// CRL supersedes the ones issued before it, and an entry of an older
// one that the newest dropped, such as a certificate taken off hold,
// is a revocation no more. Supersession is decided among the CRLs
// that one key signed, so that the CRLs of two CAs sharing a name,
// as a re-keyed CA and its predecessor do, never supersede each
// other. The CRLs keep their order otherwise.
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

// bindCRLs works out what issuer makes of the CRLs that carry its
// name, the only ones it can have signed, found in the index rather
// than by scanning them all, calling spend before each
// signature verification. A CRL that another key signed, as happens
// when two CAs share a subject, is simply not the issuer's. A CRL that
// the issuer's own key signed while the certificate is not allowed to
// sign CRLs is reported as orphaned.
func (c *crlChecker) bindCRLs(issuer *x509.Certificate, issuerName string, spend func() error) (crlBinding, error) {
	var binding crlBinding
	for _, crl := range c.crlsByIssuerName[issuerName] {
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
	binding.crls = newestCompleteCRLs(binding.crls)
	return binding, nil
}

// hasCRLFrom reports whether a CRL that covers cert carries the given
// issuer name, as rendered by nameKey.
func (c *crlChecker) hasCRLFrom(issuerName string, cert *x509.Certificate) bool {
	for _, crl := range c.crlsByIssuerName[issuerName] {
		if c.covers(crl, cert) {
			return true
		}
	}
	return false
}

// covers reports whether crl can list cert at all: a CRL that its
// issuing distribution point limits to CA certificates covers no
// end-entity certificate, and one limited to end-entity certificates
// covers no CA certificate.
func (c *crlChecker) covers(crl *x509.RevocationList, cert *x509.Certificate) bool {
	scope, scoped := c.scopes[crl]
	if !scoped {
		return true
	}
	return !(scope.OnlyContainsCACerts && !cert.IsCA) && !(scope.OnlyContainsUserCerts && cert.IsCA)
}

// issuingScope returns the issuing distribution point of crl, and
// whether it has one. The extension was parsed once already, when
// the CRL was loaded, and refused then if it could not be.
func issuingScope(crl *x509.RevocationList) (issuingDistributionPoint, bool) {
	for _, extension := range crl.Extensions {
		if extension.Id.Equal(oidIssuingDistributionPoint) {
			var scope issuingDistributionPoint
			if _, err := asn1.Unmarshal(extension.Value, &scope); err != nil {
				return issuingDistributionPoint{}, false
			}
			return scope, true
		}
	}
	return issuingDistributionPoint{}, false
}

// nameKey renders a DER encoded distinguished name for comparison
// under the X.509 matching rules, by which attribute values compare
// after the character mapping, Unicode normalization, and case
// folding of the RFC 4518 string preparation, and without regard to
// leading, trailing, and repeated whitespace, and the attributes of a
// multi-valued RDN compare as a set; that preparation is approximated
// closely, not followed to the letter. Go compares the
// names of certificates byte for byte, but a CRL
// can come from another tool than the CA certificate and encode,
// case, space, or order the same name differently, and nothing rides
// on the name alone since the CRL's signature is verified before the
// CRL is applied. Every component of the key is prefixed with its
// length, so that delimiter characters inside a value cannot make
// distinct names collide. A name that does not parse is compared as
// it is. A name that holds a value that cannot be decoded, see
// errUndecodableName, is rendered all the same, with that value as
// encoded, and reported: it cannot be compared with its other
// encodings, so nothing may ride on the rendering.
func nameKey(rawName []byte) (string, error) {
	var sequence rawRDNSequence
	if rest, err := asn1.Unmarshal(rawName, &sequence); err != nil || len(rest) > 0 {
		return string(rawName), nil
	}
	var key strings.Builder
	var undecodable error
	for _, rdn := range sequence {
		attributes := make([]string, 0, len(rdn))
		for _, attribute := range rdn {
			value := hex.EncodeToString(attribute.Value.FullBytes)
			if text, ok := directoryString(attribute.Value); ok {
				value = strings.Join(strings.Fields(cases.Fold().String(norm.NFKC.String(strings.Map(mapForMatching, text)))), " ")
			} else if attribute.Value.Class == asn1.ClassUniversal && attribute.Value.Tag == asn1.TagT61String {
				undecodable = errUndecodableName
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
	return key.String(), undecodable
}

// mapForMatching applies the character mapping of RFC 4518 section
// 2.2 to r: the characters that carry no meaning for matching, such
// as control characters and the soft hyphen, map to nothing, and
// the ones that separate words map to a space, which the whitespace
// handling then takes care of. It returns -1 for a character mapped
// to nothing, as strings.Map expects.
func mapForMatching(r rune) rune {
	switch {
	case r <= 0x0008, r >= 0x000e && r <= 0x001f, r >= 0x007f && r <= 0x0084, r >= 0x0086 && r <= 0x009f:
		return -1 // control characters
	case r == 0x06dd, r == 0x070f, r == 0x180e, r >= 0x200c && r <= 0x200f, r >= 0x202a && r <= 0x202e,
		r >= 0x2060 && r <= 0x2063, r >= 0x206a && r <= 0x206f, r == 0xfeff, r >= 0xfff9 && r <= 0xfffb,
		r >= 0x1d173 && r <= 0x1d17a, r == 0xe0001, r >= 0xe0020 && r <= 0xe007f:
		return -1 // the other characters with a control function
	case r == 0x00ad, r == 0x1806, r == 0x034f, r >= 0x180b && r <= 0x180d, r >= 0xfe00 && r <= 0xfe0f, r == 0xfffc, r == 0x200b:
		return -1 // soft hyphens, grapheme joiner, variation selectors, object replacement, zero width space
	case r >= 0x0009 && r <= 0x000d, r == 0x0085, r == 0x00a0, r == 0x1680, r >= 0x2000 && r <= 0x200a, r == 0x2028, r == 0x2029, r == 0x202f, r == 0x205f, r == 0x3000:
		return ' '
	}
	return r
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
		trusted:      map[string][]*x509.Certificate{},
		bySubject:    map[string][]*x509.Certificate{},
		indexed:      map[string]struct{}{},
		names:        map[string]renderedName{},
		crlsByIssuer: map[string]crlBinding{},
	}
	if len(verifiedChains) == 0 {
		check.chains = [][]*x509.Certificate{presented}
		check.verified = false
	}
	if check.verified {
		for _, chain := range check.chains {
			for _, cert := range chain {
				check.index(cert, check.trusted)
			}
		}
	}
	return check
}

// presentedBySubject indexes the presented certificates on first use
// and returns the index. The certificates presented beyond the chains
// being checked are not checked, but one of them may be the issuer of
// a certificate that is, such as a root presented beyond the
// intermediate that a verified chain ends at. They are only indexed
// once a checked certificate's issuer has to be looked for among
// them, so that presenting many of them costs nothing otherwise.
func (ck *crlCheck) presentedBySubject() map[string][]*x509.Certificate {
	if !ck.presentedIndexed {
		ck.presentedIndexed = true
		for _, cert := range ck.presented {
			ck.index(cert, ck.bySubject)
		}
	}
	return ck.bySubject
}

// run checks the chains, and passes the peer when any of the verified
// chains passes, as verification itself accepts the peer on the
// strength of any valid chain: an intermediate cross-signed by two
// roots may be revoked by one and not the other, as during a CA
// rollover, and the chain through the other root still carries the
// peer. A chain passes when none of its certificates fails, each of
// which is checked once, its outcome shared by the chains that hold
// it. When every chain fails, the peer fails with the first chain's
// failure.
func (ck *crlCheck) run() error {
	outcomes := make(map[string]error, len(ck.presented))
	var failure error
	for _, chain := range ck.chains {
		var chainFailure error
		for i, cert := range chain {
			anchor := ck.verified && i == len(chain)-1
			key := string(cert.Raw)
			if anchor {
				key += "|anchor"
			}
			outcome, done := outcomes[key]
			if !done {
				outcome = ck.checkCertificate(cert, anchor)
				outcomes[key] = outcome
			}
			if outcome != nil {
				chainFailure = outcome
				break
			}
		}
		if chainFailure == nil {
			return nil
		}
		if failure == nil {
			failure = chainFailure
		}
	}
	return failure
}

// checkCertificate checks one certificate of a chain against the
// CRLs of its issuer. A certificate whose issuer cannot be found while
// a CRL is configured under its issuer's name fails, so that a peer
// cannot dodge that CRL by leaving part of its chain out, unless it
// is the trust anchor a verified chain ends at: verification vouches
// for that one, and its issuer, when neither configured nor
// presented, is not the peer's to supply. A certificate whose issuer
// has no CRL configured passes, since there would be nothing to check
// it against. When a configured CRL is signed by the key of the
// certificate's issuer, one of the issuer certificates found has to
// be allowed to validate it, since a peer could otherwise present a
// forged issuer that carries the real issuer's key but not the CRL
// signing key usage. A self-signed certificate is checked like any
// other, so that recognizing it costs one of the bounded signature
// checks like any other issuer lookup.
func (ck *crlCheck) checkCertificate(cert *x509.Certificate, anchor bool) error {
	if issuer := ck.renderName(cert.RawIssuer); issuer.err != nil {
		// Nothing can tell whether a CRL applies to a certificate
		// whose issuer name cannot be compared, so the check fails
		// closed on it, as it is refused in a configured CRL or CA
		// certificate.
		return fmt.Errorf("cannot check the revocation of certificate CommonName=%s: %w", cert.Subject.CommonName, issuer.err)
	}
	lookup, err := ck.crlsFor(cert)
	if err != nil {
		return fmt.Errorf("cannot check the revocation of certificate CommonName=%s: %w", cert.Subject.CommonName, err)
	}
	if !lookup.issued {
		if !anchor && ck.checker.hasCRLFrom(ck.nameOf(cert.RawIssuer), cert) {
			return fmt.Errorf("cannot check the revocation of certificate CommonName=%s: no certificate is available for its issuer %s", cert.Subject.CommonName, cert.Issuer.CommonName)
		}
		return nil
	}
	if lookup.orphaned {
		return fmt.Errorf("cannot check the revocation of certificate CommonName=%s: a CRL signed by the key of its issuer %s is configured, but none of the certificates found for that issuer may sign CRLs", cert.Subject.CommonName, cert.Issuer.CommonName)
	}
	for _, crl := range lookup.crls {
		if ck.checker.covers(crl, cert) && ck.checker.isRevoked(cert, crl) {
			return fmt.Errorf("Certificate revoked: CommonName=%s", cert.Subject.CommonName)
		}
	}
	return nil
}

// index records cert in the given index as a possible issuer of the
// certificates that carry its subject as their issuer, once per
// distinct certificate across the indexes and leaving out the
// configured issuers, which the checker indexed, keeping the order in
// which it was indexed.
func (ck *crlCheck) index(cert *x509.Certificate, into map[string][]*x509.Certificate) {
	if _, configured := ck.checker.configuredBindings[string(cert.Raw)]; configured {
		return
	}
	if _, done := ck.indexed[string(cert.Raw)]; done {
		return
	}
	ck.indexed[string(cert.Raw)] = struct{}{}
	subject := ck.nameOf(cert.RawSubject)
	into[subject] = append(into[subject], cert)
}

// nameOf renders a DER encoded name with nameKey, once per name for
// the connection.
func (ck *crlCheck) nameOf(rawName []byte) string {
	return ck.renderName(rawName).key
}

// renderName is nameOf along with whether the name could be decoded.
func (ck *crlCheck) renderName(rawName []byte) renderedName {
	if name, done := ck.names[string(rawName)]; done {
		return name
	}
	key, err := nameKey(rawName)
	name := renderedName{key: key, err: err}
	ck.names[string(rawName)] = name
	return name
}

// crlsFor looks for a certificate that issued cert among the
// candidates that carry its issuer's name, and for the CRLs that such
// a certificate validates. The candidates that the operator or
// verification vouches for, the configured issuers and the
// certificates of the verified chains, are tried first, and the
// search stops at the first issuer that validates a CRL: every
// certificate that issued cert holds the same key, so the ones after
// it validate no other CRL. When one of those leaves a CRL of its
// key's unvalidated, for want of the CRL signing key usage, that
// verdict is final: the certificates the peer alone vouches for are
// only tried when none of the others issued cert or failed it, so
// that a wrapper the peer presents, carrying the issuer's key along
// with the key usage the issuer lacks, cannot lift the verdict. Among
// the peer's own certificates, the search carries on past an issuer
// that validates none, since that may be a forged one, and reports
// whether one of them left a CRL of its key's unvalidated. It returns
// an error, and not merely no issuer, when the connection's signature
// checks run out, so that a padded chain fails the check instead of
// hiding a certificate from it.
func (ck *crlCheck) crlsFor(cert *x509.Certificate) (crlLookup, error) {
	var lookup crlLookup
	issuerName := ck.nameOf(cert.RawIssuer)
	if !ck.checker.hasCRLFrom(issuerName, cert) {
		// Nothing could apply to cert, so its issuer is not worth
		// a signature check: not finding one changes nothing for
		// a certificate whose issuer has no CRL.
		return lookup, nil
	}
	vouchedFor := slices.Concat(ck.checker.configuredBySubject[issuerName], ck.trusted[issuerName])
	for _, candidates := range []func() []*x509.Certificate{
		func() []*x509.Certificate { return vouchedFor },
		func() []*x509.Certificate { return ck.presentedBySubject()[issuerName] },
	} {
		if lookup.orphaned {
			return lookup, nil
		}
		for _, candidate := range candidates() {
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

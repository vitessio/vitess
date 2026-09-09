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
	"slices"
	"time"

	"vitess.io/vitess/go/vt/log"
)

// crlChecker rejects a connection whose peer presents a certificate
// listed in one of the configured Certificate Revocation Lists.
type crlChecker struct {
	crls []*x509.RevocationList
	// issuers are the CA certificates configured for the connection.
	// A CRL only applies to a certificate when the CRL's signature
	// verifies from that certificate's issuer, and a peer commonly
	// presents its leaf certificate alone, so the issuer is looked
	// for here as well as among the certificates the peer sent.
	issuers []*x509.Certificate
}

func certIsRevoked(cert *x509.Certificate, crl *x509.RevocationList) bool {
	if !time.Now().Before(crl.NextUpdate) {
		log.Warn("The current Certificate Revocation List (CRL) is past expiry date and must be updated. Revoked certificates will still be rejected in this state.")
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
	checker := &crlChecker{crls: crls}
	if ca != "" {
		checker.issuers, err = loadx509Certificates(ca)
		if err != nil {
			return nil, err
		}
	}
	return checker, nil
}

// verifyConnection is a tls.Config.VerifyConnection callback. Unlike
// VerifyPeerCertificate, Go runs it on every connection: it is not
// skipped on resumed sessions, and it sees the peer's certificates
// even when InsecureSkipVerify disables Go's own chain verification,
// which is the case for every client mode short of verify_identity.
func (c *crlChecker) verifyConnection(cs tls.ConnectionState) error {
	// The peer's certificates are the ones it presented plus any that
	// a verified chain added to them, which a platform verifier can
	// do, so that every certificate below a trust anchor is checked.
	peerCerts := make([]*x509.Certificate, 0, len(cs.PeerCertificates))
	peerCerts = append(peerCerts, cs.PeerCertificates...)
	for _, chain := range cs.VerifiedChains {
		for _, cert := range chain {
			if !slices.ContainsFunc(peerCerts, cert.Equal) {
				peerCerts = append(peerCerts, cert)
			}
		}
	}
	issuers := make([]*x509.Certificate, 0, len(c.issuers)+len(peerCerts))
	issuers = append(issuers, c.issuers...)
	issuers = append(issuers, peerCerts...)

	for _, cert := range peerCerts {
		for _, issuer := range issuers {
			if !bytes.Equal(cert.RawIssuer, issuer.RawSubject) || cert.CheckSignatureFrom(issuer) != nil {
				continue
			}
			for _, crl := range c.crls {
				if crl.CheckSignatureFrom(issuer) != nil {
					continue
				}
				if certIsRevoked(cert, crl) {
					return fmt.Errorf("Certificate revoked: CommonName=%v", cert.Subject.CommonName)
				}
			}
		}
	}
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

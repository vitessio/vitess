package ssl

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	nethttp "net/http"
	"os"

	"vitess.io/vitess/go/vt/log"

	"github.com/howeyc/gopass"
)

// Determine if a string element is in a string array
func HasString(elem string, arr []string) bool {
	for _, s := range arr {
		if s == elem {
			return true
		}
	}
	return false
}

// NewTLSConfig returns an initialized TLS configuration suitable for client
// authentication. If caFile is non-empty, it will be loaded.
func NewTLSConfig(caFile string, verifyCert bool) (*tls.Config, error) {
	var c tls.Config

	// Set to TLS 1.2 as a minimum.  This is overridden for mysql communication
	c.MinVersion = tls.VersionTLS12

	if verifyCert {
		log.Info("verifyCert requested, client certificates will be verified")
		c.ClientAuth = tls.VerifyClientCertIfGiven
	}
	caPool, err := ReadCAFile(caFile)
	if err != nil {
		return &c, err
	}
	c.ClientCAs = caPool
	return &c, nil
}

// Returns CA certificate. If caFile is non-empty, it will be loaded.
func ReadCAFile(caFile string) (*x509.CertPool, error) {
	var caCertPool *x509.CertPool
	if caFile != "" {
		data, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		caCertPool = x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(data) {
			return nil, errors.New("No certificates parsed")
		}
		log.Infof("Read in CA file: %v", caFile)
	}
	return caCertPool, nil
}

// AppendKeyPair loads the given TLS key pair and appends it to
// tlsConfig.Certificates.
func AppendKeyPair(tlsConfig *tls.Config, certFile string, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
	return nil
}

// Read in a keypair where the key is password protected
func AppendKeyPairWithPassword(tlsConfig *tls.Config, certFile string, keyFile string, pemPass []byte) error {

	// Certificates aren't usually password protected, but we're kicking the password
	// along just in case.  It won't be used if the file isn't encrypted
	certData, err := ReadPEMData(certFile, pemPass)
	if err != nil {
		return err
	}
	keyData, err := ReadPEMData(keyFile, pemPass)
	if err != nil {
		return err
	}
	cert, err := tls.X509KeyPair(certData, keyData)
	if err != nil {
		return err
	}
	tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
	return nil
}

// Read a PEM file and ask for a password to decrypt it if needed
func ReadPEMData(pemFile string, pemPass []byte) ([]byte, error) {
	pemData, err := os.ReadFile(pemFile)
	if err != nil {
		return pemData, err
	}

	// We should really just get the pem.Block back here, if there's other
	// junk on the end, warn about it.
	pemBlock, rest := pem.Decode(pemData)
	if len(rest) > 0 {
		log.Warning("Didn't parse all of", pemFile)
	}

	if x509.IsEncryptedPEMBlock(pemBlock) { //nolint SA1019
		// Decrypt and get the ASN.1 DER bytes here
		pemData, err = x509.DecryptPEMBlock(pemBlock, pemPass) //nolint SA1019
		if err != nil {
			return pemData, err
		}
		log.Infof("Decrypted %v successfully", pemFile)
		// Shove the decrypted DER bytes into a new pem Block with blank headers
		var newBlock pem.Block
		newBlock.Type = pemBlock.Type
		newBlock.Bytes = pemData
		// This is now like reading in an uncrypted key from a file and stuffing it
		// into a byte stream
		pemData = pem.EncodeToMemory(&newBlock)
	}
	return pemData, nil
}

// Print a password prompt on the terminal and collect a password
func GetPEMPassword(pemFile string) []byte {
	fmt.Printf("Password for %s: ", pemFile)
	pass, err := gopass.GetPasswd()
	if err != nil {
		// We'll error with an incorrect password at DecryptPEMBlock
		return []byte("")
	}
	return pass
}

// Determine if PEM file is encrypted
func IsEncryptedPEM(pemFile string) bool {
	pemData, err := os.ReadFile(pemFile)
	if err != nil {
		return false
	}
	pemBlock, _ := pem.Decode(pemData)
	if len(pemBlock.Bytes) == 0 {
		return false
	}
	return x509.IsEncryptedPEMBlock(pemBlock) //nolint SA1019
}

// ListenAndServeTLS acts identically to http.ListenAndServeTLS, except that it
// expects TLS configuration.
// TODO: refactor so this is testable?
func ListenAndServeTLS(addr string, handler nethttp.Handler, tlsConfig *tls.Config) error {
	if addr == "" {
		// On unix Listen calls getaddrinfo to parse the port, so named ports are fine as long
		// as they exist in /etc/services
		addr = ":https"
	}
	l, err := tls.Listen("tcp", addr, tlsConfig)
	if err != nil {
		return err
	}
	return nethttp.Serve(l, handler)
}

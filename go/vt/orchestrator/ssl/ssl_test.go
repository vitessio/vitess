package ssl_test

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	nethttp "net/http"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/ssl"
)

func TestHasString(t *testing.T) {
	elem := "foo"
	a1 := []string{"bar", "foo", "baz"}
	a2 := []string{"bar", "fuu", "baz"}
	good := ssl.HasString(elem, a1)
	if !good {
		t.Errorf("Didn't find %s in array %s", elem, strings.Join(a1, ", "))
	}
	bad := ssl.HasString(elem, a2)
	if bad {
		t.Errorf("Unexpectedly found %s in array %s", elem, strings.Join(a2, ", "))
	}
}

// TODO: Build a fake CA and make sure it loads up
func TestNewTLSConfig(t *testing.T) {
	fakeCA := writeFakeFile(pemCertificate)
	defer syscall.Unlink(fakeCA)

	conf, err := ssl.NewTLSConfig(fakeCA, true)
	if err != nil {
		t.Errorf("Could not create new TLS config: %s", err)
	}
	if conf.ClientAuth != tls.VerifyClientCertIfGiven {
		t.Errorf("Client certificate verification was not enabled")
	}
	if conf.ClientCAs == nil {
		t.Errorf("ClientCA empty even though cert provided")
	}

	conf, err = ssl.NewTLSConfig("", false)
	if err != nil {
		t.Errorf("Could not create new TLS config: %s", err)
	}
	if conf.ClientAuth == tls.VerifyClientCertIfGiven {
		t.Errorf("Client certificate verification was enabled unexpectedly")
	}
	if conf.ClientCAs != nil {
		t.Errorf("Filling in ClientCA somehow without a cert")
	}
}

func TestStatus(t *testing.T) {
	var validOUs []string
	url := fmt.Sprintf("http://example.com%s", config.Config.StatusEndpoint)

	req, err := nethttp.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	config.Config.StatusOUVerify = false
	if err := ssl.Verify(req, validOUs); err != nil {
		t.Errorf("Failed even with verification off")
	}
	config.Config.StatusOUVerify = true
	if err := ssl.Verify(req, validOUs); err == nil {
		t.Errorf("Did not fail on with bad verification")
	}
}

func TestVerify(t *testing.T) {
	var validOUs []string

	req, err := nethttp.NewRequest("GET", "http://example.com/foo", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := ssl.Verify(req, validOUs); err == nil {
		t.Errorf("Did not fail on lack of TLS config")
	}

	pemBlock, _ := pem.Decode([]byte(pemCertificate))
	cert, err := x509.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	var tcs tls.ConnectionState
	req.TLS = &tcs

	if err := ssl.Verify(req, validOUs); err == nil {
		t.Errorf("Found a valid OU without any being available")
	}

	// Set a fake OU
	cert.Subject.OrganizationalUnit = []string{"testing"}

	// Pretend our request had a certificate
	req.TLS.PeerCertificates = []*x509.Certificate{cert}
	req.TLS.VerifiedChains = [][]*x509.Certificate{req.TLS.PeerCertificates}

	// Look for fake OU
	validOUs = []string{"testing"}

	if err := ssl.Verify(req, validOUs); err != nil {
		t.Errorf("Failed to verify certificate OU")
	}
}

func TestReadPEMData(t *testing.T) {
	pemCertFile := writeFakeFile(pemCertificate)
	defer syscall.Unlink(pemCertFile)
	pemPKFile := writeFakeFile(pemPrivateKey)
	defer syscall.Unlink(pemPKFile)
	pemPKWPFile := writeFakeFile(pemPrivateKeyWithPass)
	defer syscall.Unlink(pemPKWPFile)
	_, err := ssl.ReadPEMData(pemCertFile, []byte{})
	if err != nil {
		t.Errorf("Failed to decode certificate: %s", err)
	}
	pemNoPassBytes, err := ssl.ReadPEMData(pemPKFile, []byte{})
	if err != nil {
		t.Errorf("Failed to decode private key: %s", err)
	}
	pemPassBytes, err := ssl.ReadPEMData(pemPKWPFile, []byte("testing"))
	if err != nil {
		t.Errorf("Failed to decode private key with password: %s", err)
	}
	if reflect.DeepEqual(pemPassBytes, pemNoPassBytes) {
		t.Errorf("PEM encoding failed after password removal")
	}
}

func TestAppendKeyPair(t *testing.T) {
	c, err := ssl.NewTLSConfig("", false)
	if err != nil {
		t.Fatal(err)
	}
	pemCertFile := writeFakeFile(pemCertificate)
	defer syscall.Unlink(pemCertFile)
	pemPKFile := writeFakeFile(pemPrivateKey)
	defer syscall.Unlink(pemPKFile)

	if err := ssl.AppendKeyPair(c, pemCertFile, pemPKFile); err != nil {
		t.Errorf("Failed to append certificate and key to tls config: %s", err)
	}
}

func TestAppendKeyPairWithPassword(t *testing.T) {
	c, err := ssl.NewTLSConfig("", false)
	if err != nil {
		t.Fatal(err)
	}
	pemCertFile := writeFakeFile(pemCertificate)
	defer syscall.Unlink(pemCertFile)
	pemPKFile := writeFakeFile(pemPrivateKeyWithPass)
	defer syscall.Unlink(pemPKFile)

	if err := ssl.AppendKeyPairWithPassword(c, pemCertFile, pemPKFile, []byte("testing")); err != nil {
		t.Errorf("Failed to append certificate and key to tls config: %s", err)
	}
}

func TestIsEncryptedPEM(t *testing.T) {
	pemPKFile := writeFakeFile(pemPrivateKey)
	defer syscall.Unlink(pemPKFile)
	pemPKWPFile := writeFakeFile(pemPrivateKeyWithPass)
	defer syscall.Unlink(pemPKWPFile)
	if ssl.IsEncryptedPEM(pemPKFile) {
		t.Errorf("Incorrectly identified unencrypted PEM as encrypted")
	}
	if !ssl.IsEncryptedPEM(pemPKWPFile) {
		t.Errorf("Incorrectly identified encrypted PEM as unencrypted")
	}
}

func writeFakeFile(content string) string {
	f, err := ioutil.TempFile("", "ssl_test")
	if err != nil {
		return ""
	}
	ioutil.WriteFile(f.Name(), []byte(content), 0644)
	return f.Name()
}

const pemCertificate = `-----BEGIN CERTIFICATE-----
MIIDtTCCAp2gAwIBAgIJAOxKC7FsJelrMA0GCSqGSIb3DQEBBQUAMEUxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIEwpTb21lLVN0YXRlMSEwHwYDVQQKExhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTcwODEwMTQ0MjM3WhcNMTgwODEwMTQ0MjM3WjBF
MQswCQYDVQQGEwJBVTETMBEGA1UECBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50
ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB
CgKCAQEA12vHV3gYy5zd1lujA7prEhCSkAszE6E37mViWhLQ63CuedZfyYaTAHQK
HYDZi4K1MNAySUfZRMcICSSsxlRIz6mzXrFsowaJgwx4cbMDIvXE03KstuXoTYJh
+xmXB+5yEVEtIyP2DvPqfCmwCZb3k94Y/VY1nAQDxIxciXrAxT9zT1oYd0YWr2yp
J2mgsfnY4c3zg7W5WgvOTmYz7Ey7GJjpUjGdayx+P1CilKzSWH1xZuVQFNLSHvcH
WXkEoCMVc0tW5mO5eEO1aNHo9MSjPF386l1rq+pz5OwjqCEZq2b1YxesyLnbF+8+
iYGfYmFaDLFwG7zVDwialuI4TzIIOQIDAQABo4GnMIGkMB0GA1UdDgQWBBQ1ubGx
Yvn3wN5VXyoR0lOD7ARzVTB1BgNVHSMEbjBsgBQ1ubGxYvn3wN5VXyoR0lOD7ARz
VaFJpEcwRTELMAkGA1UEBhMCQVUxEzARBgNVBAgTClNvbWUtU3RhdGUxITAfBgNV
BAoTGEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZIIJAOxKC7FsJelrMAwGA1UdEwQF
MAMBAf8wDQYJKoZIhvcNAQEFBQADggEBALmm4Zw/4jLKDJciUGUYOcr5Xe9TP/Cs
afH7IWvaFUDfV3W6yAm9jgNfIy9aDLpuu2CdEb+0qL2hdmGLV7IM3y62Ve0UTdGV
BGsm1zMmIguew2wGbAwGr5LmIcUseatVUKAAAfDrBNwotEAdM8kmGekUZfOM+J9D
FoNQ62C0buRHGugtu6zWAcZNOe6CI7HdhaAdxZlgn8y7dfJQMacoK0NcWeUVQwii
6D4mgaqUGM2O+WcquD1vEMuBPYVcKhi43019E0+6LI5QB6w80bARY8K7tkTdRD7U
y1/C7iIqyuBVL45OdSabb37TfGlHZIPIwLaGw3i4Mr0+F0jQT8rZtTQ=
-----END CERTIFICATE-----`

const pemPrivateKey = `-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA12vHV3gYy5zd1lujA7prEhCSkAszE6E37mViWhLQ63CuedZf
yYaTAHQKHYDZi4K1MNAySUfZRMcICSSsxlRIz6mzXrFsowaJgwx4cbMDIvXE03Ks
tuXoTYJh+xmXB+5yEVEtIyP2DvPqfCmwCZb3k94Y/VY1nAQDxIxciXrAxT9zT1oY
d0YWr2ypJ2mgsfnY4c3zg7W5WgvOTmYz7Ey7GJjpUjGdayx+P1CilKzSWH1xZuVQ
FNLSHvcHWXkEoCMVc0tW5mO5eEO1aNHo9MSjPF386l1rq+pz5OwjqCEZq2b1Yxes
yLnbF+8+iYGfYmFaDLFwG7zVDwialuI4TzIIOQIDAQABAoIBAHLf4pleTbqmmBWr
IC7oxhgIBmAR2Nbq7eyO2/e0ePxURnZqPwI0ZUekmZBKGbgvp3e0TlyNl+r5R+u4
RvosD/fNQv2IF6qH3eSoTcIz98Q40xD+4eNWjp5mnOFOMB/mo6VgaHWIw7oNkElN
4bX7b2LG2QSfaE8eRPQW9XHKp+mGhYFbxgPYxUmlIXuYZF61hVwxysDA6DP3LOi8
yUL6E64x6NqN9xtg/VoN+f6N0MOvsr4yb5+uvni1LVRFI7tNqIN4Y6P6trgKfnRR
EpZeAUu8scqyxE4NeqnnjK/wBuXxaeh3e9mN1V2SzT629c1InmmQasZ5slcCJQB+
38cswgECgYEA+esaLKwHXT4+sOqMYemi7TrhxtNC2f5OAGUiSRVmTnum2gl4wOB+
h5oLZAuG5nBEIoqbMEbI35vfuHqIe390IJtPdQlz4TGDsPufYj/gnnBBFy/c8f+n
f/CdRDRYrpnpKGwvUntLRB2pFbe2hlqqq+4YUqiHauJMOCJnPbOo1lECgYEA3KnF
VOXyY0fKD45G7ttfAcpw8ZI2gY99sCRwtBQGsbO61bvw5sl/3j7AmYosz+n6f7hb
uHmitIuPv4z3r1yfVysh80tTGIM3wDkpr3fLYRxpVOZU4hgxMQV9yyaSA/Hfqn48
vIK/NC4bERqpofNNdrIqNaGWkd87ZycvpRfa0WkCgYBztbVVr4RtWG9gLAg5IRot
KhD0pEWUdpiYuDpqifznI3r6Al6lNot+rwTNGkUoFhyFvZTigjNozFuFpz3fqAAV
RLNCJdFAF1O4spd1vst5r9GDMcbjSJG9u6KkvHO+y0XXUFeMoccUT4NEqd1ZUUsp
9T/PrXWdOA9AAjW4rKDkMQKBgQC9R4NVR8mbD8Frhoeh69qbFqO7E8hdalBN/3QN
hAAZ/imNnSEPVliwsvNSwQufbPzLAcDrhKrkY7JyhOERM0oa44zDvSESLbxszpvL
P97c9hoEEW9OYaIQgr1cvUES0S8ieBZxPVX11HazPUO0/5a68ijyyCD4D5xM53gf
DU9NwQKBgQCmVthQi65xcc4mgCIwXtBZWXeaPv5x0dLEXIC5EoN6eXLK9iW//7cE
hhawtJtl+J6laB+TkEGQsyhc4v85WcywdisyR7LR7CUqFYJMKeE/VtTVKnYbfq54
rHoQS9YotByBwPtRx0V93gkc+KWBOGmSBBxKj7lrBkYkcWAiRfpJjg==
-----END RSA PRIVATE KEY-----`

const pemPrivateKeyWithPass = `-----BEGIN RSA PRIVATE KEY-----
Proc-Type: 4,ENCRYPTED
DEK-Info: DES-EDE3-CBC,3EABF60A784F9065

IDGYvdRJXvBt5vEDI9caEYJ2vvVmoqmxTKvheNX0aLSXUl/p8hIZ25kd/4mpmI3m
irQdEe2JuNh4/fPDe6Agg6mX6mYCVbiupfXdFKkqJzndW/O5nEQ4yuRgi0fO4wcH
OM/kTS8/7UaKfCuWFa71ywh1WeStFDBwsMQqLdFFeuQ/JC6g2tZW6xzCBE0BVIkq
6OWXmWumXMufhOdpb9sNoc3lbdOi037V886o0cIRQp4qPepElhhhplrhaJZBSxiP
TUldExbtYCN1APhrgUp1RpxIWHNLezjhUYLGooxb6SqinpLd9ia2uFotwNDeX7/T
dMPQPtgdFwvoCtWn9oVWp+regdZPacABLsvtTD4NS8h13BKzBmAqtYfHJk44u/Tv
6PcCb9xHI7+YpNJznrHiCtALWkfG56mDjp0SP+OKjsYMjo317D+x892i2XT79k2T
0IM0OUPizVkN5c7uDQBHqxmE9JVQT7QFMy1P57nWPsmG5o7e9Y/klaPQzi04FWEh
YAEZrU5/FQlFziu3/Jw6WwQnm3IqJP6iMlnR9Y5iZCZQnLhcJNIxxOJ/+cVH4dVD
jIHztasHgbfld045Ua7nk91VyFP5pWRPFacJ74D+xm/1IjF/+9Uj3NQX88Swig0Q
Fi7+eJ1XtCI0YdUqiUdp8QaS1GnFzibSIcXCbLLEn0Cgh/3CFXUyh92M4GIgvmcI
/hi4nUDa3nLYDHyOZubFLERb+Zr3EFzNXX4Ga3fcNH0deluxW4tda+QCk0ud6k9N
y2bCcAVnvbB+yX2s7CSVq+eaT/4JLIJY5AlrISRwYtG57SR/DN9HuU99dD30k581
PmarIt4VAakjXo/Zqd1AMh+ofbC/Qm7jBwbPGPZAM/FjpnVsvaXsdChI19Az72v3
wiLOKEw8M23vV4/E7QwW3Pp/RPyUZk6HAlBuLXbcyZHOOV4WPsKrI46BBXL8Qf4X
5kpRITFFUaFu3aaO7mloVAoneEKusKJgKOAwWifRI3jf6fH9B8qDA0jQpWRNpLs4
3A2qrOyHQ9SMoBr7ya8Vs2BMdfqAmOyiUdVzLr2EjnRxa7f3/7/sdzD1aaIJa2TM
kjpKgFMq5B/FRVmuAvKyEF52A/b6L9EpinyB53DzWnIw9W5zdjjRkuxmGmv1R94A
gJvbONh955cinHft0rm0hdKo77wDvXZdX5ZeITjOwJ0d/VBHYDGUonDVgnAVLcz+
n1BS+oOS1xLG/EJOGqtNYihVuCkbIwwdAVhc7pKo3nIbLyrKFKFyh/Br11PPBris
nlWo8BWSoFv7gKOftkulHJFAVekisaXe4OIcYMATeLvDfAnBDJrNHZn0HcyHI51L
3EhCCPJrrmfNv+QMdPk6LTts5YIdhNRSV5PR2X8ZshChod7atyrw+Wm+LCcy3h1G
xIVNracpnna+Ic5M8EIJZgLOH7IjDFS1EcPjz5em0rVqGGsLDvxmRo2ZJTPSHlpM
8q6VJEIso5sfoauf+fX+y7xk1CpFG8NkXSplbiYmZXdB1zepV1a/ZiW2uU7hEAV7
oMEzoBEIw3wTuRasixjH7Z6i8PvF3eUKXCIt0UiwTmWdCCW37c5eqjguyp9aLDtc
-----END RSA PRIVATE KEY-----`

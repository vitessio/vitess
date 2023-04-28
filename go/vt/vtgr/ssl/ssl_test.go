package ssl_test

import (
	"crypto/tls"
	"os"
	"syscall"
	"testing"

	"vitess.io/vitess/go/vt/vtgr/ssl"
)

/*
	This file has been copied over from VTOrc package
*/

// TODO: Build a fake CA and make sure it loads up
func TestNewTLSConfig(t *testing.T) {
	fakeCA := writeFakeFile(pemCertificate)
	defer syscall.Unlink(fakeCA)

	conf, err := ssl.NewTLSConfig(fakeCA, true, tls.VersionTLS13)
	if err != nil {
		t.Errorf("Could not create new TLS config: %s", err)
	}
	if conf.ClientAuth != tls.VerifyClientCertIfGiven {
		t.Errorf("Client certificate verification was not enabled")
	}
	if conf.ClientCAs == nil {
		t.Errorf("ClientCA empty even though cert provided")
	}
	if conf.MinVersion != tls.VersionTLS13 {
		t.Errorf("incorrect tls min version set")
	}

	conf, err = ssl.NewTLSConfig("", false, tls.VersionTLS12)
	if err != nil {
		t.Errorf("Could not create new TLS config: %s", err)
	}
	if conf.ClientAuth == tls.VerifyClientCertIfGiven {
		t.Errorf("Client certificate verification was enabled unexpectedly")
	}
	if conf.ClientCAs != nil {
		t.Errorf("Filling in ClientCA somehow without a cert")
	}
	if conf.MinVersion != tls.VersionTLS12 {
		t.Errorf("incorrect tls min version set")
	}
}

func TestAppendKeyPair(t *testing.T) {
	c, err := ssl.NewTLSConfig("", false, tls.VersionTLS12)
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

func writeFakeFile(content string) string {
	f, err := os.CreateTemp("", "ssl_test")
	if err != nil {
		return ""
	}
	os.WriteFile(f.Name(), []byte(content), 0644)
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

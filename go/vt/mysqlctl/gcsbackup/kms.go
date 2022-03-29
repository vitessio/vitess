package gcsbackup

import (
	"context"
	"encoding/base64"

	cloudkms "google.golang.org/api/cloudkms/v1"
)

// KMS implements GCP encrypter/decrypter.
type kms struct {
	uri     string
	service *cloudkms.Service
}

// NewKMS returns a new KMS service.
func newKMS(uri string, srv *cloudkms.Service) *kms {
	return &kms{
		uri:     uri,
		service: srv,
	}
}

// Encrypt implementation.
func (k *kms) encrypt(ctx context.Context, pt []byte) ([]byte, error) {
	req := k.service.Projects.Locations.KeyRings.CryptoKeys.Encrypt(k.uri, &cloudkms.EncryptRequest{
		Plaintext: base64.StdEncoding.EncodeToString(pt),
	})

	resp, err := req.Context(ctx).Do()
	if err != nil {
		return nil, err
	}

	return []byte(resp.Ciphertext), nil
}

// Decrypt implementation.
func (k *kms) decrypt(ctx context.Context, ct []byte) ([]byte, error) {
	req := k.service.Projects.Locations.KeyRings.CryptoKeys.Decrypt(k.uri, &cloudkms.DecryptRequest{
		Ciphertext: string(ct),
	})

	resp, err := req.Context(ctx).Do()
	if err != nil {
		return nil, err
	}

	return base64.StdEncoding.DecodeString(resp.Plaintext)
}

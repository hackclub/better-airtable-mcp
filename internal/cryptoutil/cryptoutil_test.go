package cryptoutil

import (
	"bytes"
	"strings"
	"testing"
)

func TestCipherRoundTrip(t *testing.T) {
	secret, err := New([]byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	ciphertext, err := secret.Encrypt([]byte("hello"))
	if err != nil {
		t.Fatalf("Encrypt() returned error: %v", err)
	}
	if bytes.Equal(ciphertext, []byte("hello")) {
		t.Fatal("expected ciphertext to differ from plaintext")
	}

	plaintext, err := secret.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt() returned error: %v", err)
	}
	if string(plaintext) != "hello" {
		t.Fatalf("expected plaintext %q, got %q", "hello", plaintext)
	}
}

func TestNewRejectsInvalidKeyLength(t *testing.T) {
	if _, err := New([]byte("short")); err == nil {
		t.Fatal("expected invalid key length to be rejected")
	}
}

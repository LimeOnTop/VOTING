package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/LimeOnTop/VOTING/internal/apperr"
	"github.com/LimeOnTop/VOTING/internal/usecase"
)

const tokenIDBytes = 16
const minimumSigningKeyBytes = 32

type IDs struct{}

func (IDs) NewID() (string, error) {
	var b [tokenIDBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("random id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

var _ usecase.IDGenerator = IDs{}

type AnonymousIdentity struct {
	key []byte
	ids usecase.IDGenerator
}

var _ usecase.AnonymousIdentity = (*AnonymousIdentity)(nil)

func NewAnonymousIdentity(signingKey string, ids usecase.IDGenerator) (*AnonymousIdentity, error) {
	if len(signingKey) < minimumSigningKeyBytes || ids == nil {
		return nil, fmt.Errorf("signing key must contain at least %d bytes and id generator is required", minimumSigningKeyBytes)
	}
	return &AnonymousIdentity{key: []byte(signingKey), ids: ids}, nil
}
func (i *AnonymousIdentity) sign(value string) string {
	mac := hmac.New(sha256.New, i.key)
	_, _ = mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))
}
func (i *AnonymousIdentity) NewToken() (string, error) {
	id, err := i.ids.NewID()
	if err != nil {
		return "", err
	}
	return id + "." + i.sign(id), nil
}
func (i *AnonymousIdentity) ParseToken(token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 2 || len(parts[0]) != tokenIDBytes*2 {
		return "", apperr.ErrInvalidIdentity
	}
	if _, err := hex.DecodeString(parts[0]); err != nil {
		return "", apperr.ErrInvalidIdentity
	}
	if !hmac.Equal([]byte(parts[1]), []byte(i.sign(parts[0]))) {
		return "", apperr.ErrInvalidIdentity
	}
	return parts[0], nil
}
func (i *AnonymousIdentity) VoterKey(pollID, browserID string) string {
	return i.sign(pollID + ":" + browserID)
}

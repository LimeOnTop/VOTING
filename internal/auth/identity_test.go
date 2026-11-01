package auth

import (
	"errors"
	"strings"
	"testing"

	"github.com/LimeOnTop/VOTING/internal/apperr"
)

func TestIdentityAuthenticityAndPollScope(t *testing.T) {
	identity, err := NewAnonymousIdentity(strings.Repeat("s", 32), IDs{})
	if err != nil {
		t.Fatal(err)
	}
	token, err := identity.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	id, err := identity.ParseToken(token)
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{"", token + "x", "x" + token[1:], id + ".invalid"} {
		if _, err = identity.ParseToken(invalid); !errors.Is(err, apperr.ErrInvalidIdentity) {
			t.Fatalf("accepted invalid token: %v", err)
		}
	}
	if identity.VoterKey("poll-a", id) == identity.VoterKey("poll-b", id) {
		t.Fatal("identity is not poll scoped")
	}
	other, err := NewAnonymousIdentity(strings.Repeat("t", 32), IDs{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = other.ParseToken(token); !errors.Is(err, apperr.ErrInvalidIdentity) {
		t.Fatal("accepted another signing key")
	}
}

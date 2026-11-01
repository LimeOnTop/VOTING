package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"github.com/LimeOnTop/VOTING/internal/domain"
	"net/http"
	"strings"
)

func mac(secret, value string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(value))
	return hex.EncodeToString(h.Sum(nil))
}
func (s *Server) cookieValue() string { id := domain.ID(); return id + "." + mac(s.SigningKey, id) }
func (s *Server) identity(r *http.Request) (string, bool) {
	c, err := r.Cookie("voting_id")
	if err != nil {
		return "", false
	}
	parts := strings.Split(c.Value, ".")
	if len(parts) != 2 || len(parts[0]) != 32 {
		return "", false
	}
	if _, err = hex.DecodeString(parts[0]); err != nil {
		return "", false
	}
	if !hmac.Equal([]byte(parts[1]), []byte(mac(s.SigningKey, parts[0]))) {
		return "", false
	}
	return parts[0], true
}
func (s *Server) voterKey(poll, id string) string { return mac(s.SigningKey, poll+":"+id) }
func (s *Server) admin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		got, bearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		a := sha256.Sum256([]byte(got))
		b := sha256.Sum256([]byte(s.AdminToken))
		if !bearer || !hmac.Equal(a[:], b[:]) {
			fail(w, 401, "нужен ключ администратора")
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		next(w, r)
	}
}

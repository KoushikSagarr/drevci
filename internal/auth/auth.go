package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

var ErrUnauthorized = errors.New("unauthorized")

// OrgContext carries the authenticated organisation info through the request context.
type OrgContext struct {
	OrgID       string
	OrgSlug     string
	Plan        string
	WorkerLimit int
}

type contextKey string

// OrgContextKey is the key used to store OrgContext in the request context.
const OrgContextKey contextKey = "org"

// WithOrgContext returns a new request with OrgContext stored in its context.
func WithOrgContext(r *http.Request, org *OrgContext) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), OrgContextKey, org))
}

// GetOrgContext retrieves OrgContext from the request context (nil if not set).
func GetOrgContext(r *http.Request) *OrgContext {
	v := r.Context().Value(OrgContextKey)
	if v == nil {
		return nil
	}
	org, _ := v.(*OrgContext)
	return org
}

// HashToken returns the SHA-256 hex digest of a raw token string.
// Always hash before storing or comparing tokens.
func HashToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// Config holds static tokens for the legacy single-tenant mode.
type Config struct {
	Tokens []string
}

// TokenLookup is an optional callback for the SaaS token path.
// It receives a SHA-256 token hash and returns (orgID, slug, plan, workerLimit, error).
type TokenLookup func(ctx context.Context, tokenHash string) (orgID, slug, plan string, workerLimit int, err error)

// Middleware returns an HTTP middleware that validates Bearer tokens.
//
// Two modes (tried in order):
//  1. Static token — checked against Config.Tokens (single-tenant / local dev).
//  2. DB token     — tokenHash looked up via lookup callback (SaaS / Postgres mode).
//
// On success the OrgContext (if available) is injected into the request context
// and the X-Org-ID header is set on the response.
func Middleware(cfg Config, lookup ...TokenLookup) func(http.Handler) http.Handler {
	staticTokens := make(map[string]bool, len(cfg.Tokens))
	for _, t := range cfg.Tokens {
		staticTokens[t] = true
	}

	var dbLookup TokenLookup
	if len(lookup) > 0 {
		dbLookup = lookup[0]
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := extractToken(r)

			if token == "" {
				sendUnauthorized(w, "missing token")
				return
			}

			// ── Mode 1: static token (backwards-compatible) ───────────────
			if staticTokens[token] {
				next.ServeHTTP(w, r)
				return
			}

			// ── Mode 2: DB token lookup ───────────────────────────────────
			if dbLookup != nil {
				hash := HashToken(token)
				orgID, slug, plan, workerLimit, err := dbLookup(r.Context(), hash)
				if err == nil {
					org := &OrgContext{
						OrgID:       orgID,
						OrgSlug:     slug,
						Plan:        plan,
						WorkerLimit: workerLimit,
					}
					r = WithOrgContext(r, org)
					w.Header().Set("X-Org-ID", orgID)
					next.ServeHTTP(w, r)
					return
				}
			}

			sendUnauthorized(w, "invalid token")
		})
	}
}

// extractToken pulls the Bearer token from the Authorization header or
// falls back to the ?token query parameter (for EventSource / SSE clients).
func extractToken(r *http.Request) string {
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) == 2 && strings.ToLower(parts[0]) == "bearer" {
			return parts[1]
		}
	}
	return r.URL.Query().Get("token")
}

func sendUnauthorized(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// GenerateToken creates a cryptographically random 32-byte hex token.
func GenerateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating token: %w", err)
	}
	return hex.EncodeToString(b), nil
}

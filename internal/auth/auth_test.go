package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExtractToken(t *testing.T) {
	t.Run("valid bearer header", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/runs", nil)
		req.Header.Set("Authorization", "Bearer my-secret-token")
		tok := extractToken(req)
		if tok != "my-secret-token" {
			t.Errorf("expected my-secret-token, got %q", tok)
		}
	})

	t.Run("case-insensitive bearer prefix", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/runs", nil)
		req.Header.Set("Authorization", "bearer lowercase-token")
		tok := extractToken(req)
		if tok != "lowercase-token" {
			t.Errorf("expected lowercase-token, got %q", tok)
		}
	})

	t.Run("query token alone is rejected", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/runs?token=query-secret-token", nil)
		tok := extractToken(req)
		if tok != "" {
			t.Errorf("SECURITY DEFECT: query token was extracted: %q", tok)
		}
	})

	t.Run("missing header and missing query returns empty", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/runs", nil)
		tok := extractToken(req)
		if tok != "" {
			t.Errorf("expected empty string, got %q", tok)
		}
	})

	t.Run("non-bearer authorization header returns empty", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/runs", nil)
		req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
		tok := extractToken(req)
		if tok != "" {
			t.Errorf("expected empty string for Basic auth, got %q", tok)
		}
	})

	t.Run("bearer header with query token present prefers header", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/runs?token=query-ignored", nil)
		req.Header.Set("Authorization", "Bearer header-token")
		tok := extractToken(req)
		if tok != "header-token" {
			t.Errorf("expected header-token, got %q", tok)
		}
	})
}

func TestMiddleware(t *testing.T) {
	cfg := Config{Tokens: []string{"valid-static-token"}}

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	mw := Middleware(cfg)(nextHandler)

	t.Run("valid bearer token accepted", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/runs", nil)
		req.Header.Set("Authorization", "Bearer valid-static-token")
		rr := httptest.NewRecorder()

		mw.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("expected 200 OK, got %d", rr.Code)
		}
	})

	t.Run("missing token is unauthorized", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/runs", nil)
		rr := httptest.NewRecorder()

		mw.ServeHTTP(rr, req)

		if rr.Code != http.StatusUnauthorized {
			t.Errorf("expected 401 Unauthorized, got %d", rr.Code)
		}

		var body map[string]string
		json.NewDecoder(rr.Body).Decode(&body)
		if body["error"] != "missing token" {
			t.Errorf("expected 'missing token' error message, got %q", body["error"])
		}
	})

	t.Run("invalid bearer token is unauthorized", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/runs", nil)
		req.Header.Set("Authorization", "Bearer wrong-token")
		rr := httptest.NewRecorder()

		mw.ServeHTTP(rr, req)

		if rr.Code != http.StatusUnauthorized {
			t.Errorf("expected 401 Unauthorized, got %d", rr.Code)
		}

		var body map[string]string
		json.NewDecoder(rr.Body).Decode(&body)
		if body["error"] != "invalid token" {
			t.Errorf("expected 'invalid token' error message, got %q", body["error"])
		}
	})

	t.Run("query token alone is unauthorized", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/runs?token=valid-static-token", nil)
		rr := httptest.NewRecorder()

		mw.ServeHTTP(rr, req)

		if rr.Code != http.StatusUnauthorized {
			t.Errorf("SECURITY DEFECT: query token alone was accepted, got status %d", rr.Code)
		}

		var body map[string]string
		json.NewDecoder(rr.Body).Decode(&body)
		if body["error"] != "missing token" {
			t.Errorf("expected 'missing token', got %q", body["error"])
		}
	})

	t.Run("valid bearer token works when query token is also present", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/runs?token=some-other-token", nil)
		req.Header.Set("Authorization", "Bearer valid-static-token")
		rr := httptest.NewRecorder()

		mw.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("expected 200 OK with valid bearer header, got %d", rr.Code)
		}
	})

	t.Run("DB lookup with query token alone is unauthorized", func(t *testing.T) {
		lookupCalled := false
		dbLookup := TokenLookup(func(ctx context.Context, tokenHash string) (string, string, string, int, error) {
			lookupCalled = true
			return "org-1", "org-slug", "pro", 5, nil
		})

		dbMW := Middleware(Config{}, dbLookup)(nextHandler)

		req, _ := http.NewRequest("GET", "/api/v1/runs?token=some-db-token", nil)
		rr := httptest.NewRecorder()

		dbMW.ServeHTTP(rr, req)

		if rr.Code != http.StatusUnauthorized {
			t.Errorf("expected 401 Unauthorized, got %d", rr.Code)
		}
		if lookupCalled {
			t.Errorf("SECURITY DEFECT: db lookup was called for query token")
		}
	})
}

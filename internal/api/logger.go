package api

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-chi/chi/v5/middleware"
)

// SanitizedLogFormatter implements chi's middleware.LogFormatter,
// formatting request logs while strictly redacting sensitive query parameters.
type SanitizedLogFormatter struct {
	Logger *log.Logger
}

// NewLogEntry initializes a log entry for an incoming HTTP request,
// creating a sanitized URI representation where sensitive query parameters are redacted.
func (s *SanitizedLogFormatter) NewLogEntry(r *http.Request) middleware.LogEntry {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	host := r.Host
	if host == "" {
		host = r.URL.Host
	}

	sanitizedURI := r.URL.Path
	if sanitizedQuery := SanitizeQuery(r.URL.RawQuery); sanitizedQuery != "" {
		sanitizedURI += "?" + sanitizedQuery
	}

	var reqURI string
	if host != "" {
		reqURI = fmt.Sprintf("%s://%s%s", scheme, host, sanitizedURI)
	} else {
		reqURI = sanitizedURI
	}

	logger := s.Logger
	if logger == nil {
		logger = log.New(os.Stdout, "", log.LstdFlags)
	}

	return &sanitizedLogEntry{
		logger:     logger,
		method:     r.Method,
		reqURI:     reqURI,
		proto:      r.Proto,
		remoteAddr: r.RemoteAddr,
	}
}

type sanitizedLogEntry struct {
	logger     *log.Logger
	method     string
	reqURI     string
	proto      string
	remoteAddr string
}

func (e *sanitizedLogEntry) Write(status, bytes int, header http.Header, elapsed time.Duration, extra interface{}) {
	e.logger.Printf("%q from %s - %03d %dB in %s",
		fmt.Sprintf("%s %s %s", e.method, e.reqURI, e.proto),
		e.remoteAddr,
		status,
		bytes,
		elapsed,
	)
}

func (e *sanitizedLogEntry) Panic(v interface{}, stack []byte) {
	middleware.PrintPrettyStack(v)
}

// SanitizeQuery redacts sensitive query parameters case-insensitively,
// while preserving non-sensitive query parameters and parameter order.
func SanitizeQuery(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	parts := strings.Split(rawQuery, "&")
	sanitized := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		eqIdx := strings.Index(part, "=")
		var key string
		if eqIdx != -1 {
			key = part[:eqIdx]
		} else {
			key = part
		}

		unescapedKey, err := url.QueryUnescape(key)
		if err != nil {
			unescapedKey = key
		}

		if isSensitiveParam(unescapedKey) {
			sanitized = append(sanitized, key+"=[REDACTED]")
		} else {
			sanitized = append(sanitized, part)
		}
	}
	return strings.Join(sanitized, "&")
}

func isSensitiveParam(key string) bool {
	k := strings.ToLower(strings.TrimSpace(key))
	normalized := strings.ReplaceAll(strings.ReplaceAll(k, "-", ""), "_", "")
	switch normalized {
	case "token", "accesstoken", "apikey", "secret", "password", "auth", "credential", "bearer":
		return true
	default:
		return false
	}
}

package auth

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPersistEnvSecrets_CreateNew(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, ".env")

	updates := map[string]string{
		"DREV_TOKEN":          "test-token-val",
		"DREV_WEBHOOK_SECRET": "test-webhook-val",
	}

	if err := PersistEnvSecrets(envPath, updates); err != nil {
		t.Fatalf("PersistEnvSecrets failed: %v", err)
	}

	content, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	str := string(content)
	if !strings.Contains(str, "DREV_TOKEN=test-token-val") {
		t.Errorf("expected DREV_TOKEN in output, got: %s", str)
	}
	if !strings.Contains(str, "DREV_WEBHOOK_SECRET=test-webhook-val") {
		t.Errorf("expected DREV_WEBHOOK_SECRET in output, got: %s", str)
	}

	info, err := os.Stat(envPath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	// Check mode is 0600 (ignoring directory/type bits)
	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Errorf("expected permissions 0600, got %o", perm)
	}
}

func TestPersistEnvSecrets_PreserveExisting(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, ".env")

	initial := `# Configuration file
SOME_OTHER_VAR=important_value
DREV_PORT=9090

# Existing token
DREV_TOKEN=old_token
`
	if err := os.WriteFile(envPath, []byte(initial), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	updates := map[string]string{
		"DREV_TOKEN":          "new_token",
		"DREV_WEBHOOK_SECRET": "new_secret",
	}

	if err := PersistEnvSecrets(envPath, updates); err != nil {
		t.Fatalf("PersistEnvSecrets failed: %v", err)
	}

	content, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	str := string(content)
	if !strings.Contains(str, "# Configuration file") {
		t.Errorf("comment was lost: %s", str)
	}
	if !strings.Contains(str, "SOME_OTHER_VAR=important_value") {
		t.Errorf("unrelated var was lost: %s", str)
	}
	if !strings.Contains(str, "DREV_PORT=9090") {
		t.Errorf("DREV_PORT was lost: %s", str)
	}
	if !strings.Contains(str, "DREV_TOKEN=new_token") {
		t.Errorf("DREV_TOKEN was not updated: %s", str)
	}
	if strings.Contains(str, "DREV_TOKEN=old_token") {
		t.Errorf("old token still present: %s", str)
	}
	if !strings.Contains(str, "DREV_WEBHOOK_SECRET=new_secret") {
		t.Errorf("DREV_WEBHOOK_SECRET was not added: %s", str)
	}

	// Verify only one occurrence of DREV_TOKEN
	if strings.Count(str, "DREV_TOKEN=") != 1 {
		t.Errorf("DREV_TOKEN duplicated: %s", str)
	}
}

func TestPersistEnvSecrets_NoLeakOnError(t *testing.T) {
	// Trying to write into a non-existent directory without permissions
	invalidPath := "/path/to/nonexistent/dir/.env"
	secretVal := "super-confidential-secret-12345"

	err := PersistEnvSecrets(invalidPath, map[string]string{
		"DREV_TOKEN": secretVal,
	})
	if err == nil {
		t.Fatal("expected error for invalid path, got nil")
	}

	errMsg := err.Error()
	if strings.Contains(errMsg, secretVal) {
		t.Fatalf("error message leaked secret value: %s", errMsg)
	}
}

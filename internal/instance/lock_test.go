package instance

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func TestAcquireLocalWorkspaceLock_MutualExclusion(t *testing.T) {
	wsRoot := t.TempDir()

	lock1, err := AcquireLocalWorkspaceLock(wsRoot)
	if err != nil {
		t.Fatalf("first AcquireLocalWorkspaceLock failed: %v", err)
	}
	defer lock1.Close()

	// Second acquire on the same workspace root must fail immediately
	lock2, err := AcquireLocalWorkspaceLock(wsRoot)
	if err == nil {
		lock2.Close()
		t.Fatalf("expected second AcquireLocalWorkspaceLock to fail, but succeeded")
	}
	if !strings.Contains(err.Error(), "already active for workspace root") {
		t.Fatalf("unexpected error message: %v", err)
	}

	// Release first lock
	if err := lock1.Close(); err != nil {
		t.Fatalf("closing first lock failed: %v", err)
	}

	// Now third acquire must succeed
	lock3, err := AcquireLocalWorkspaceLock(wsRoot)
	if err != nil {
		t.Fatalf("third AcquireLocalWorkspaceLock failed after release: %v", err)
	}
	defer lock3.Close()
}

func TestCanonicalizeDBPath(t *testing.T) {
	tmpDir := t.TempDir()
	rawDB := filepath.Join(tmpDir, "mydb.db")

	canon1, err := CanonicalizeDBPath(rawDB)
	if err != nil {
		t.Fatalf("CanonicalizeDBPath failed: %v", err)
	}

	if !filepath.IsAbs(canon1) {
		t.Fatalf("expected absolute path, got %q", canon1)
	}

	// Create symlink pointing to the dir
	symDir := filepath.Join(tmpDir, "symdir")
	if err := os.Symlink(tmpDir, symDir); err != nil {
		t.Fatal(err)
	}

	symDB := filepath.Join(symDir, "mydb.db")
	canon2, err := CanonicalizeDBPath(symDB)
	if err != nil {
		t.Fatalf("CanonicalizeDBPath through symlink failed: %v", err)
	}

	if canon1 != canon2 {
		t.Fatalf("expected canonical paths to match: %q != %q", canon1, canon2)
	}
}

func TestAcquireSQLiteInstanceLock_MutualExclusion(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	lock1, err := AcquireSQLiteInstanceLock(dbPath)
	if err != nil {
		t.Fatalf("first AcquireSQLiteInstanceLock failed: %v", err)
	}
	defer lock1.Close()

	// Second acquire must fail
	lock2, err := AcquireSQLiteInstanceLock(dbPath)
	if err == nil {
		lock2.Close()
		t.Fatalf("expected second AcquireSQLiteInstanceLock to fail, but succeeded")
	}
	if !strings.Contains(err.Error(), "already active for SQLite database") {
		t.Fatalf("unexpected error message: %v", err)
	}

	// Release first lock
	if err := lock1.Close(); err != nil {
		t.Fatalf("closing first lock failed: %v", err)
	}

	// Re-acquire succeeds
	lock3, err := AcquireSQLiteInstanceLock(dbPath)
	if err != nil {
		t.Fatalf("re-acquiring SQLite instance lock failed: %v", err)
	}
	defer lock3.Close()
}

func TestPostgresLeaseKeeper_LinearizationOnLoss(t *testing.T) {
	// Unit test for PostgresLeaseKeeper state linearization:
	// Verify that if a genuine loss occurs, Release() returns that lossErr
	// and does not classify it as graceful shutdown.
	lossErr := fmt.Errorf("simulated network severance")

	k := &PostgresLeaseKeeper{
		key1:        PostgresLockKey1,
		key2:        PostgresLockKey2,
		state:       stateLost,
		lossErr:     lossErr,
		doneCh:      make(chan struct{}),
		failStopCh:  make(chan error, 1),
	}
	close(k.doneCh) // watchdog already exited

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := k.Release(ctx)
	if err != lossErr {
		t.Fatalf("expected Release() to return genuine lossErr %v, got %v", lossErr, err)
	}
}

func setConnSearchPath(connStr, schema string) string {
	searchPathVal := fmt.Sprintf("%s,public", schema)
	if strings.HasPrefix(connStr, "postgres://") || strings.HasPrefix(connStr, "postgresql://") {
		u, err := url.Parse(connStr)
		if err == nil {
			q := u.Query()
			q.Set("search_path", searchPathVal)
			u.RawQuery = q.Encode()
			return u.String()
		}
	}
	return connStr + fmt.Sprintf(" search_path='%s'", searchPathVal)
}

func TestPostgresLeaseKeeper_Integration(t *testing.T) {
	connStr := os.Getenv("DREV_DB_URL")
	if connStr == "" {
		t.Skip("DREV_DB_URL not set — skipping postgres advisory lease integration test")
	}

	schema := fmt.Sprintf("test_lease_%d_%d", time.Now().UnixNano(), rand.Intn(10000))
	scopedConnStr := setConnSearchPath(connStr, schema)

	db, err := sql.Open("postgres", scopedConnStr)
	if err != nil {
		t.Fatalf("opening postgres: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("cannot connect to postgres: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Acquire lease
	keeper, err := AcquirePostgresLease(ctx, db)
	if err != nil {
		t.Fatalf("AcquirePostgresLease failed: %v", err)
	}

	// 2. Verify pg_locks has exactly the lock with objsubid = 2
	var lockCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM pg_locks
		WHERE locktype = 'advisory'
		  AND classid = $1::oid
		  AND objid = $2::oid
		  AND objsubid = 2
		  AND pid = $3
		  AND granted = true
	`, PostgresLockKey1, PostgresLockKey2, keeper.backendPID).Scan(&lockCount)
	if err != nil {
		_ = keeper.Release(ctx)
		t.Fatalf("querying pg_locks: %v", err)
	}
	if lockCount != 1 {
		_ = keeper.Release(ctx)
		t.Fatalf("expected exactly 1 advisory lock with objsubid=2, got %d", lockCount)
	}

	// 3. Second acquire on same DB must fail
	_, err2 := AcquirePostgresLease(ctx, db)
	if err2 == nil {
		_ = keeper.Release(ctx)
		t.Fatalf("expected second AcquirePostgresLease to fail, but it succeeded")
	}
	if !strings.Contains(err2.Error(), "database deployment lease is already held") {
		_ = keeper.Release(ctx)
		t.Fatalf("unexpected error message on second lease: %v", err2)
	}

	// 4. Release keeper
	if err := keeper.Release(ctx); err != nil {
		t.Fatalf("keeper.Release failed: %v", err)
	}

	// 5. Verify lock is no longer in pg_locks
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM pg_locks
		WHERE locktype = 'advisory'
		  AND classid = $1::oid
		  AND objid = $2::oid
		  AND objsubid = 2
		  AND pid = $3
	`, PostgresLockKey1, PostgresLockKey2, keeper.backendPID).Scan(&lockCount)
	if err != nil {
		t.Fatalf("querying pg_locks after release: %v", err)
	}
	if lockCount != 0 {
		t.Fatalf("expected 0 locks after release, got %d", lockCount)
	}
}

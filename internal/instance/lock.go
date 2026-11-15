package instance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Default PostgreSQL advisory lock keys (two 32-bit positive signed integers)
// key1: 0x64726576 ("drev" = 1685218678)
// key2: 0x63696461 ("cida" = 1667851361)
const (
	PostgresLockKey1 int32 = 1685218678
	PostgresLockKey2 int32 = 1667851361
)

// AcquireLocalWorkspaceLock acquires an exclusive kernel advisory lock on <wsRoot>/.drevd_instance.lock.
func AcquireLocalWorkspaceLock(wsRoot string) (*os.File, error) {
	canonicalRoot, err := filepath.EvalSymlinks(filepath.Clean(wsRoot))
	if err != nil {
		return nil, fmt.Errorf("canonicalizing workspace root: %w", err)
	}

	lockPath := filepath.Join(canonicalRoot, ".drevd_instance.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("opening local workspace instance lockfile: %w", err)
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("another drevd daemon instance is already active for workspace root %q", wsRoot)
	}

	return f, nil
}

// CanonicalizeDBPath resolves symlinks and absolute paths for SQLite databases.
func CanonicalizeDBPath(rawPath string) (string, error) {
	if strings.TrimSpace(rawPath) == "" {
		rawPath = "./drev.db"
	}

	absPath, err := filepath.Abs(rawPath)
	if err != nil {
		return "", fmt.Errorf("resolving absolute database path: %w", err)
	}

	realPath, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			dir := filepath.Dir(absPath)
			realDir, dirErr := filepath.EvalSymlinks(dir)
			if dirErr != nil {
				realDir = dir
			}
			realPath = filepath.Join(realDir, filepath.Base(absPath))
		} else {
			return "", fmt.Errorf("evaluating database symlinks: %w", err)
		}
	}

	return filepath.Clean(realPath), nil
}

// AcquireSQLiteInstanceLock acquires an exclusive kernel advisory lock on <canonicalDBPath>.instance.lock.
func AcquireSQLiteInstanceLock(dbPath string) (*os.File, error) {
	canonicalPath, err := CanonicalizeDBPath(dbPath)
	if err != nil {
		return nil, err
	}

	lockPath := canonicalPath + ".instance.lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("opening SQLite instance lockfile: %w", err)
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("another drevd daemon instance is already active for SQLite database %q", canonicalPath)
	}

	return f, nil
}

type keeperState int

const (
	stateActive keeperState = iota
	stateReleasing
	stateReleased
	stateLost
)

// PostgresLeaseKeeper manages a dedicated PostgreSQL advisory lock connection and its liveness watchdog.
type PostgresLeaseKeeper struct {
	conn       *sql.Conn
	key1       int32
	key2       int32
	backendPID int

	mu      sync.Mutex
	state   keeperState
	lossErr error

	watchdogCtx    context.Context
	watchdogCancel context.CancelFunc

	doneCh     chan struct{}
	failStopCh chan error
	closeOnce  sync.Once
}

// AcquirePostgresLease acquires a dedicated session advisory lock on PostgreSQL using (key1, key2).
func AcquirePostgresLease(ctx context.Context, db *sql.DB) (*PostgresLeaseKeeper, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("allocating dedicated keeper connection: %w", err)
	}

	var backendPID int
	var acquired bool
	err = conn.QueryRowContext(ctx, "SELECT pg_backend_pid(), pg_try_advisory_lock($1::int4, $2::int4)", PostgresLockKey1, PostgresLockKey2).Scan(&backendPID, &acquired)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("acquiring advisory lock: %w", err)
	}
	if !acquired {
		_ = conn.Close()
		return nil, errors.New("database deployment lease is already held by another active drevd daemon")
	}

	wCtx, wCancel := context.WithCancel(context.Background())
	k := &PostgresLeaseKeeper{
		conn:           conn,
		key1:           PostgresLockKey1,
		key2:           PostgresLockKey2,
		backendPID:     backendPID,
		state:          stateActive,
		watchdogCtx:    wCtx,
		watchdogCancel: wCancel,
		doneCh:         make(chan struct{}),
		failStopCh:     make(chan error, 1),
	}

	go k.watchdog()
	return k, nil
}

func (k *PostgresLeaseKeeper) watchdog() {
	defer close(k.doneCh)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-k.watchdogCtx.Done():
			return
		case <-ticker.C:
			k.mu.Lock()
			if k.state != stateActive {
				k.mu.Unlock()
				return
			}
			k.mu.Unlock()

			pingCtx, pingCancel := context.WithTimeout(k.watchdogCtx, 3*time.Second)
			var currentPID int
			var isHeld bool
			queryErr := k.conn.QueryRowContext(pingCtx, `
				SELECT pg_backend_pid(), EXISTS (
					SELECT 1 FROM pg_locks 
					WHERE locktype = 'advisory' 
					  AND classid = $1::oid 
					  AND objid = $2::oid 
					  AND objsubid = 2 
					  AND pid = pg_backend_pid()
					  AND granted = true
				)`, k.key1, k.key2).Scan(&currentPID, &isHeld)
			pingCancel()

			isGenuineLoss := false
			var failErr error
			if queryErr != nil {
				if !errors.Is(queryErr, context.Canceled) {
					isGenuineLoss = true
					failErr = fmt.Errorf("watchdog ping failed: %w", queryErr)
				}
			} else if currentPID != k.backendPID {
				isGenuineLoss = true
				failErr = fmt.Errorf("backend session PID changed (expected %d, got %d)", k.backendPID, currentPID)
			} else if !isHeld {
				isGenuineLoss = true
				failErr = errors.New("PostgreSQL advisory lock is no longer held by keeper session")
			}

			k.mu.Lock()
			if isGenuineLoss {
				k.state = stateLost
				k.lossErr = failErr
				k.mu.Unlock()
				select {
				case k.failStopCh <- failErr:
				default:
				}
				return
			}

			if k.state != stateActive {
				k.mu.Unlock()
				return
			}
			k.mu.Unlock()
		}
	}
}

// Release gracefully unlocks and closes the dedicated PostgreSQL keeper connection.
func (k *PostgresLeaseKeeper) Release(ctx context.Context) (err error) {
	k.closeOnce.Do(func() {
		k.mu.Lock()
		if k.state == stateLost {
			err = k.lossErr
			k.mu.Unlock()
			if k.watchdogCancel != nil {
				k.watchdogCancel()
			}
			if k.doneCh != nil {
				<-k.doneCh
			}
			if k.conn != nil {
				_ = k.conn.Close()
			}
			return
		}
		k.state = stateReleasing
		k.mu.Unlock()

		if k.watchdogCancel != nil {
			k.watchdogCancel()
		}

		if k.doneCh != nil {
			select {
			case <-k.doneCh:
			case <-time.After(5 * time.Second):
				log.Fatalf("FATAL: PostgreSQL watchdog failed to terminate within release deadline; halting to prevent concurrent connection use")
			}
		}

		k.mu.Lock()
		defer k.mu.Unlock()

		if k.state == stateLost {
			err = k.lossErr
			if k.conn != nil {
				_ = k.conn.Close()
			}
			return
		}

		k.state = stateReleased

		if k.conn != nil {
			unlockCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			var released bool
			_ = k.conn.QueryRowContext(unlockCtx, "SELECT pg_advisory_unlock($1::int4, $2::int4)", k.key1, k.key2).Scan(&released)

			err = k.conn.Close()
		}
	})
	return err
}

// FailStopChannel returns the channel signaling unexpected lease loss.
func (k *PostgresLeaseKeeper) FailStopChannel() <-chan error {
	return k.failStopCh
}

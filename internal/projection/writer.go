package projection

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

var ErrWriterOwnership = errors.New("PROJECTION_WRITER_OWNERSHIP")

// Keys cover the physical namespace, rather than a version/job alias. All
// cooperating writers for the same target must use this same source metadata DB.
func TargetKey(component, endpoint, namespace string) string {
	endpoint = strings.TrimRight(endpoint, "/")
	sum := sha256.Sum256([]byte(component + "\x00" + endpoint + "\x00" + namespace))
	return hex.EncodeToString(sum[:])
}
func VersionTargetKey(v Version, component string) string {
	if component == "ck" {
		return TargetKey(component, v.Contract.CK.Endpoint, v.Contract.CK.Database)
	}
	return TargetKey(component, v.Contract.ES.Endpoint, v.Contract.ES.Index)
}
func TargetLockName(key string) string { return "tp:target:" + key[:48] }

type WriterClaim struct {
	Key       string
	Component string
	Kind      string
	Owner     string
}
type TargetOwner struct {
	Key          string    `json:"target_key"`
	Component    string    `json:"component"`
	Kind         string    `json:"owner_kind"`
	Owner        string    `json:"owner_id"`
	Epoch        int64     `json:"owner_epoch"`
	HandoffID    *string   `json:"handoff_id"`
	VersionID    *string   `json:"version_id"`
	Token        *string   `json:"worker_token,omitempty"`
	Host         *string   `json:"worker_host"`
	PID          *int64    `json:"worker_pid"`
	ConnectionID *int64    `json:"lock_connection_id"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func ReadOwner(ctx context.Context, q rowQuery, key, lock string) (o TargetOwner, err error) {
	err = q.QueryRowContext(ctx, `SELECT target_key,component,owner_kind,owner_id,owner_epoch,handoff_id,version_id,worker_token,worker_host,worker_pid,lock_connection_id,updated_at FROM sync_projection_target_owners WHERE target_key=?`+lock, key).Scan(&o.Key, &o.Component, &o.Kind, &o.Owner, &o.Epoch, &o.HandoffID, &o.VersionID, &o.Token, &o.Host, &o.PID, &o.ConnectionID, &o.UpdatedAt)
	return
}

type WriterLease struct {
	mu           sync.Mutex // QueryRow+Scan must finish before another query on this Conn.
	db           *sql.DB
	conn         *sql.Conn
	Claim        WriterClaim
	Token        string
	Epoch        int64
	ConnectionID int64
	VersionID    string
	HandoffID    string
}

// A free MySQL named lock alone is insufficient: a lost connection can leave a
// live worker with an ES/CK request. Reclaim only a proved-dead local PID, or a
// cleanly released token. Remote/unknown/live owners are never auto-preempted.
func previousWorkerDead(o TargetOwner, host string) bool {
	if o.Token == nil {
		return true
	}
	if o.Host == nil || *o.Host != host || o.PID == nil || *o.PID <= 0 {
		return false
	}
	return errors.Is(syscall.Kill(int(*o.PID), 0), syscall.ESRCH)
}
func AcquireWriter(ctx context.Context, db *sql.DB, c WriterClaim, allowLegacy bool) (*WriterLease, error) {
	fail := func(err error) (*WriterLease, error) { return nil, fmt.Errorf("%w: %v", ErrWriterOwnership, err) }
	if !hashPattern.MatchString(c.Key) || (c.Component != "es" && c.Component != "ck") || (c.Kind != "rebuild_job" && c.Kind != "consumer_group") || c.Owner == "" {
		return fail(fmt.Errorf("invalid writer claim"))
	}
	// Probe before acquiring: legacy environments without migration 012 are not
	// enrolled or changed by ordinary rebuild commands.
	_, probe := ReadOwner(ctx, db, c.Key, "")
	if MissingTable(probe) && allowLegacy {
		installed, err := HandoffInstalled(ctx, db)
		if err != nil {
			return fail(err)
		}
		if installed {
			return fail(fmt.Errorf("HANDOFF_METADATA_UNAVAILABLE: installed writer ownership table unavailable"))
		}
		return nil, nil
	}
	if probe != nil && !errors.Is(probe, sql.ErrNoRows) {
		return fail(probe)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fail(err)
	}
	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		conn.ExecContext(cleanupCtx, "SELECT RELEASE_LOCK(?)", TargetLockName(c.Key))
		conn.Close()
	}
	var locked int
	if err = conn.QueryRowContext(ctx, "SELECT GET_LOCK(?,0)", TargetLockName(c.Key)).Scan(&locked); err != nil || locked != 1 {
		cleanup()
		return fail(fmt.Errorf("target writer already running or lock unavailable: %v", err))
	}
	var cid int64
	if err = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&cid); err != nil {
		cleanup()
		return fail(err)
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		cleanup()
		return fail(err)
	}
	defer tx.Rollback()
	releaseConn := cleanup
	cleanup = func() { tx.Rollback(); releaseConn() }
	o, err := ReadOwner(ctx, tx, c.Key, " FOR UPDATE")
	if errors.Is(err, sql.ErrNoRows) && c.Kind == "rebuild_job" {
		_, err = tx.ExecContext(ctx, `INSERT INTO sync_projection_target_owners(target_key,component,owner_kind,owner_id,owner_epoch,updated_at) VALUES(?,?,?,?,1,UTC_TIMESTAMP(6))`, c.Key, c.Component, c.Kind, c.Owner)
		if err == nil {
			o, err = ReadOwner(ctx, tx, c.Key, " FOR UPDATE")
		}
	}
	if err != nil {
		cleanup()
		return fail(err)
	}
	if o.Component != c.Component || o.Kind != c.Kind || o.Owner != c.Owner {
		cleanup()
		return fail(fmt.Errorf("target belongs to %s/%s", o.Kind, o.Owner))
	}
	host, err := os.Hostname()
	if err != nil {
		cleanup()
		return fail(err)
	}
	if !previousWorkerDead(o, host) {
		cleanup()
		return fail(fmt.Errorf("previous target writer termination is unproved; refusing takeover"))
	}
	var raw [16]byte
	if _, err = rand.Read(raw[:]); err != nil {
		cleanup()
		return fail(err)
	}
	token := hex.EncodeToString(raw[:])
	epoch := o.Epoch + 1
	_, err = tx.ExecContext(ctx, `UPDATE sync_projection_target_owners SET owner_epoch=?,worker_token=?,worker_host=?,worker_pid=?,lock_connection_id=?,updated_at=UTC_TIMESTAMP(6) WHERE target_key=?`, epoch, token, host, os.Getpid(), cid, c.Key)
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		cleanup()
		return fail(fmt.Errorf("writer registration result unknown: %w", err))
	}
	lease := &WriterLease{db: db, conn: conn, Claim: c, Token: token, Epoch: epoch, ConnectionID: cid}
	if o.VersionID != nil {
		lease.VersionID = *o.VersionID
	}
	if o.HandoffID != nil {
		lease.HandoffID = *o.HandoffID
	}
	return lease, nil
}
func (w *WriterLease) Check(ctx context.Context) error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.check(ctx)
}
func (w *WriterLease) check(ctx context.Context) error {
	var held int
	err := w.conn.QueryRowContext(ctx, "SELECT COALESCE(IS_USED_LOCK(?)=CONNECTION_ID(),0)", TargetLockName(w.Claim.Key)).Scan(&held)
	if err != nil || held != 1 {
		return fmt.Errorf("%w: target lock connection lost: %v", ErrWriterOwnership, err)
	}
	o, err := ReadOwner(ctx, w.db, w.Claim.Key, "")
	if err != nil || o.Kind != w.Claim.Kind || o.Owner != w.Claim.Owner || o.Epoch != w.Epoch || o.Token == nil || *o.Token != w.Token || o.ConnectionID == nil || *o.ConnectionID != w.ConnectionID {
		return fmt.Errorf("%w: persistent writer no longer matches lease: %v", ErrWriterOwnership, err)
	}
	var version, handoff string
	if o.VersionID != nil {
		version = *o.VersionID
	}
	if o.HandoffID != nil {
		handoff = *o.HandoffID
	}
	if version != w.VersionID || handoff != w.HandoffID {
		return fmt.Errorf("%w: writer version/handoff linkage changed", ErrWriterOwnership)
	}
	return nil
}
func (w *WriterLease) Close() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Only a still-valid lock holder can clear its token. Losing the connection
	// leaves the token for explicit death verification on a subsequent process.
	if w.check(ctx) == nil {
		w.conn.ExecContext(ctx, `UPDATE sync_projection_target_owners SET worker_token=NULL,worker_host=NULL,worker_pid=NULL,lock_connection_id=NULL,updated_at=UTC_TIMESTAMP(6) WHERE target_key=? AND worker_token=? AND owner_epoch=?`, w.Claim.Key, w.Token, w.Epoch)
	}
	w.conn.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", TargetLockName(w.Claim.Key))
	w.conn.Close()
}
func (w *WriterLease) Transition(ctx context.Context, tx *sql.Tx, kind, owner, handoffID, versionID string) error {
	if err := w.Check(ctx); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE sync_projection_target_owners SET owner_kind=?,owner_id=?,owner_epoch=owner_epoch+1,handoff_id=?,version_id=?,worker_token=NULL,worker_host=NULL,worker_pid=NULL,lock_connection_id=NULL,updated_at=UTC_TIMESTAMP(6) WHERE target_key=? AND owner_epoch=? AND worker_token=?`, kind, owner, handoffID, versionID, w.Claim.Key, w.Epoch, w.Token)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: owner transition rejected", ErrWriterOwnership)
	}
	return nil
}

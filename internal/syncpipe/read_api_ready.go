package syncpipe

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/mysqlstore"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/readruntime"
	"ticketpulse/internal/search"
	"ticketpulse/internal/ticket"
	"time"
)

// Credentials are supplied by the operator on each observation and never
// persisted. Search probes are a bounded id-sorted page of current permission.
type ReadAPIProbe struct {
	BootID          string            `json:"boot_id"`
	Token           string            `json:"-"`
	Analytics       analytics.Request `json:"analytics_request"`
	PermissionToken string            `json:"-"`
}
type ReadReady struct {
	OperationID        string             `json:"operation_id"`
	VersionID          string             `json:"version_id"`
	ReadRevision       int64              `json:"read_revision"`
	FreezeID           string             `json:"freeze_id"`
	FreezeRevision     int64              `json:"freeze_revision"`
	API                projection.APIBoot `json:"api"`
	Search             search.Result      `json:"search"`
	Analytics          analytics.Result   `json:"analytics"`
	PermissionSearch   *search.Result     `json:"permission_search,omitempty"`
	AnalyticsForbidden bool               `json:"analytics_current_permission_refused"`
	RequestURLs        []string           `json:"request_urls"`
	Audit              ReconcileReport    `json:"reconcile_report"`
	ObservedAt         time.Time          `json:"observed_at"`
}

func probeHTTP(ctx context.Context, base, path, token string, identity projection.APIIdentity, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		return 0, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := &http.Client{Timeout: 12 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return fmt.Errorf("API probe redirects forbidden") }}
	res, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.Header.Get("X-TicketPulse-Boot-ID") != identity.BootID || res.Header.Get("X-TicketPulse-Read-Version") != identity.VersionID || res.Header.Get("X-TicketPulse-Read-Revision") != strconv.FormatInt(identity.ReadRevision, 10) || res.Header.Get("X-TicketPulse-Status-Scope") != identity.StateNamespace {
		return res.StatusCode, fmt.Errorf("READ_API_PROBE_MISMATCH: HTTP process/selection identity differs")
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, 2<<20))
	if err != nil {
		return res.StatusCode, err
	}
	if out != nil && res.StatusCode == http.StatusOK {
		if err = json.Unmarshal(data, out); err != nil {
			return res.StatusCode, err
		}
	}
	return res.StatusCode, nil
}
func sameJSON(a, b any) bool {
	ad, _ := json.Marshal(a)
	bd, _ := json.Marshal(b)
	return bytes.Equal(ad, bd)
}
func (p *ReadSwitchControl) checkReadReady(ctx context.Context, s projection.ReadSelection, o projection.ReadSwitch, probe ReadAPIProbe) (ReadReady, error) {
	return p.checkReadReadyMode(ctx, s, o, probe, true)
}
func (p *ReadSwitchControl) checkReadReadyMode(ctx context.Context, s projection.ReadSelection, o projection.ReadSwitch, probe ReadAPIProbe, requireBoundary bool) (ReadReady, error) {
	result := ReadReady{OperationID: o.ID, VersionID: s.VersionID, ReadRevision: s.ReadRevision, FreezeID: o.FreezeID, FreezeRevision: o.FreezeRevision, ObservedAt: time.Now().UTC()}
	if probe.Token == "" || probe.BootID == "" {
		return result, fmt.Errorf("READ_API_READY_REQUIRED: current boot and operator probe credential required")
	}
	b, err := p.readStore().APIBoot(ctx, probe.BootID)
	if err != nil {
		return result, err
	}
	result.API = b
	i := b.Identity
	if b.State != "RUNNING" || time.Since(b.HeartbeatAt) > 10*time.Second || i.VersionID != s.VersionID || i.ContractHash != s.ContractSHA256 || i.ReadRevision != s.ReadRevision || !reflect.DeepEqual(i.HandoffID, s.HandoffID) {
		return result, fmt.Errorf("READ_API_READY_REQUIRED: boot selection/state differs")
	}
	if err = readruntime.Alive(ctx, i.Process); err != nil {
		return result, err
	}
	e, err := p.readStore().Execution(ctx, o.ID)
	if err != nil {
		return result, err
	}
	var inventory ReadDrainScope
	if json.Unmarshal(e.DrainScope, &inventory) != nil {
		return result, projection.ErrReadControl
	}
	processes, err := readruntime.Scan(ctx, inventory.Scope)
	if err != nil {
		return result, err
	}
	seen := false
	for _, process := range processes {
		if process.PID == i.Process.PID {
			if process.Started != i.Process.Started || process.SHA256 != i.Process.SHA256 {
				return result, fmt.Errorf("READ_API_PROCESS_CHANGED")
			}
			seen = true
			continue
		}
		// Multiple newly started APIs require a later explicitly verified inventory;
		// the first release permits one new API, never an unnoticed old executable.
		return result, fmt.Errorf("READ_API_PROCESS_BLOCKED: additional unobserved API process")
	}
	if !seen {
		return result, fmt.Errorf("READ_API_PROCESS_BLOCKED: new API absent from controlled scope")
	}
	d, err := p.ResolveReadDestination(ctx, s.VersionID)
	if err != nil {
		return result, err
	}
	if d.CKDatabaseUUID != o.CKDatabaseUUID || i.CKDatabaseUUID != o.CKDatabaseUUID || i.StateNamespace != d.StateNamespace || !reflect.DeepEqual(i.Groups, d.Groups) {
		return result, fmt.Errorf("READ_API_STATUS_SCOPE_CHANGED")
	}
	if requireBoundary {
		if _, _, _, err = p.waitReadMarker(ctx, d, o); err != nil {
			return result, err
		}
	}
	for _, component := range []string{"es", "ck"} {
		if _, err = p.waitReadWriter(ctx, d, component); err != nil {
			return result, err
		}
	}
	host, _, err := net.SplitHostPort(i.Process.Listen)
	if err != nil || host != "127.0.0.1" {
		return result, fmt.Errorf("API probe requires a declared loopback listener")
	}
	base := "http://" + i.Process.Listen
	status, err := probeHTTP(ctx, base, "/readyz", "", i, nil)
	if err != nil || status != 200 {
		return result, fmt.Errorf("READ_API_READY_REQUIRED: readyz failed status=%d: %v", status, err)
	}
	var actual projection.APIIdentity
	status, err = probeHTTP(ctx, base, "/api/v1/read-target", probe.Token, i, &actual)
	if err != nil || status != 200 || !sameJSON(actual, i) {
		return result, fmt.Errorf("READ_API_READY_REQUIRED: process target response differs: %v", err)
	}
	auth := mysqlstore.New(p.Store.DB)
	uid, err := auth.Authenticate(ctx, probe.Token)
	if err != nil {
		return result, err
	}
	user, _, err := auth.AnalyticsAccess(ctx, uid)
	if err != nil || !user.IsManager() {
		return result, fmt.Errorf("API proof requires current manager authorization")
	}
	searchPath := "/api/v1/search/tickets?sort=id&order=asc&page_size=100"
	result.RequestURLs = append(result.RequestURLs, base+"/readyz", base+"/api/v1/read-target", base+searchPath)
	status, err = probeHTTP(ctx, base, searchPath, probe.Token, i, &result.Search)
	if err != nil || status != 200 {
		return result, fmt.Errorf("READ_API_SEARCH_PROBE_FAILED: %d: %v", status, err)
	}
	if err = checkProbeSearch(ctx, p.Store.DB, auth, uid, result.Search); err != nil {
		return result, err
	}
	f := result.Search.Freshness
	if f.VersionID != s.VersionID || f.ReadRevision != s.ReadRevision || f.StateNamespace != d.StateNamespace || f.ConsumerGroup != d.Groups["es"] {
		return result, fmt.Errorf("READ_API_SEARCH_STATUS_MISMATCH")
	}
	query := url.Values{"start": {probe.Analytics.Start}, "end": {probe.Analytics.End}}
	for _, id := range probe.Analytics.TeamIDs {
		query.Add("team_id", strconv.FormatInt(id, 10))
	}
	analyticsPath := "/api/v1/analytics?" + query.Encode()
	result.RequestURLs = append(result.RequestURLs, base+analyticsPath)
	status, err = probeHTTP(ctx, base, analyticsPath, probe.Token, i, &result.Analytics)
	if err != nil || status != 200 {
		return result, fmt.Errorf("READ_API_ANALYTICS_PROBE_FAILED: %d: %v", status, err)
	}
	a := result.Analytics.Sync
	if a.VersionID != s.VersionID || a.ReadRevision != s.ReadRevision || a.StateNamespace != d.StateNamespace || a.ConsumerGroup != d.Groups["ck"] {
		return result, fmt.Errorf("READ_API_CK_STATUS_MISMATCH")
	}
	// Full facts and independently computed MySQL metrics verify the API's CK
	// result without using the production CK SQL to generate an expected result.
	result.Audit, err = Reconcile(ctx, p.Store, nil, d.ES, d.CK, ReconcileOptions{BatchSize: 250, MetricsOrganizationID: user.OrganizationID, MetricsRequest: probe.Analytics, ConfirmSourceBoundary: d.CheckIdentities})
	if err != nil || result.Audit.State != ReconcileConsistent || result.Audit.DifferenceCount != 0 || result.Audit.Metrics == nil {
		return result, fmt.Errorf("READ_API_RECONCILE_FAILED: %v", err)
	}
	var committedProof ReadSwitchProof
	if requireBoundary && (json.Unmarshal(o.Proof, &committedProof) != nil || committedProof.Report.Source != result.Audit.Source || result.Audit.SourceAfter == nil || committedProof.Report.SourceAfter == nil || *result.Audit.SourceAfter != *committedProof.Report.SourceAfter) {
		return result, fmt.Errorf("READ_SOURCE_BOUNDARY_CHANGED: source no longer matches committed frozen proof")
	}
	expected := analytics.Overview{}
	var duration float64
	for _, row := range result.Audit.Metrics.MySQL {
		switch row.Metric {
		case "created":
			expected.Created += row.Amount
		case "resolved":
			expected.FirstResolved += row.Amount
			duration += row.Duration
		case "backlog":
			expected.Backlog += row.Amount
		}
	}
	if expected.FirstResolved > 0 {
		value := duration / float64(expected.FirstResolved)
		expected.AverageFirstResolutionSeconds = &value
	}
	if !sameJSON(expected, result.Analytics.Overview) {
		return result, fmt.Errorf("READ_API_ANALYTICS_RESULT_MISMATCH")
	}
	if probe.PermissionToken != "" {
		permissionUID, err := auth.Authenticate(ctx, probe.PermissionToken)
		if err != nil {
			return result, err
		}
		permission := search.Result{}
		status, err = probeHTTP(ctx, base, searchPath, probe.PermissionToken, i, &permission)
		if err != nil || status != 200 {
			return result, fmt.Errorf("READ_API_PERMISSION_SEARCH_FAILED")
		}
		if err = checkProbeSearch(ctx, p.Store.DB, auth, permissionUID, permission); err != nil {
			return result, err
		}
		result.PermissionSearch = &permission
		status, err = probeHTTP(ctx, base, analyticsPath, probe.PermissionToken, i, nil)
		if err != nil || status != 403 {
			return result, fmt.Errorf("READ_API_CURRENT_MANAGER_PERMISSION_FAILED")
		}
		result.AnalyticsForbidden = true
	}
	if err = readruntime.Alive(ctx, i.Process); err != nil {
		return result, err
	}
	current, err := p.readStore().APIBoot(ctx, i.BootID)
	if err != nil || current.State != "RUNNING" || current.IdentityHash != b.IdentityHash {
		return result, fmt.Errorf("READ_API_PROCESS_CHANGED")
	}
	return result, nil
}
func checkProbeSearch(ctx context.Context, db *sql.DB, auth *mysqlstore.Store, uid int64, result search.Result) error {
	if result.Total != nil || result.TotalRelation != "unavailable" {
		return fmt.Errorf("READ_API_UNAUTHORIZED_TOTAL")
	}
	access, err := auth.BeginSearch(ctx, uid)
	if err != nil {
		return err
	}
	defer access.Close()
	ids := []int64{}
	seen := map[int64]bool{}
	last := int64(0)
	for _, item := range result.Items {
		if seen[item.ID] || item.ID <= last {
			return fmt.Errorf("READ_API_SEARCH_SORT_MISMATCH")
		}
		seen[item.ID] = true
		last = item.ID
		ids = append(ids, item.ID)
	}
	allowed, err := access.Authorized(ctx, ids)
	if err != nil {
		return err
	}
	for _, item := range result.Items {
		a, ok := allowed[item.ID]
		if !ok || !sameJSON(a, item) {
			return fmt.Errorf("READ_API_SEARCH_CURRENT_PERMISSION_VERSION_MISMATCH")
		}
	}
	// Independent current-MySQL first-page expectation. This is a bounded
	// operator probe, and does not expand the HTTP search candidate budget.
	expected := []ticket.TicketSummary{}
	lastID := int64(0)
	scanned := 0
	exhausted := false
	for scanned < 2000 && len(expected) < 101 {
		rows, err := db.QueryContext(ctx, "SELECT id FROM tickets WHERE organization_id=? AND id>? ORDER BY id LIMIT 100", access.Identity().OrganizationID, lastID)
		if err != nil {
			return err
		}
		batch := []int64{}
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		allowed, err := access.Authorized(ctx, batch)
		if err != nil {
			return err
		}
		for _, id := range batch {
			if item, ok := allowed[id]; ok {
				expected = append(expected, item)
				if len(expected) == 101 {
					break
				}
			}
			lastID = id
		}
		scanned += len(batch)
		if len(batch) < 100 {
			exhausted = true
			break
		}
	}
	if !exhausted && len(expected) < 101 && scanned >= 2000 {
		return fmt.Errorf("READ_API_SEARCH_PROBE_BUDGET: current-authority completeness not established")
	}
	hasMore := len(expected) > 100
	if hasMore {
		expected = expected[:100]
	}
	if result.PageSize != 100 || result.HasMore != hasMore || !sameJSON(expected, result.Items) {
		return fmt.Errorf("READ_API_SEARCH_COMPLETE_PAGE_MISMATCH")
	}
	return nil
}
func (p *ReadSwitchControl) ObserveAPIReady(ctx context.Context, id string, probe ReadAPIProbe) (projection.ReadSwitch, error) {
	release, err := p.lock(ctx)
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	defer release()
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	defer tx.Rollback()
	g, err := projection.LockFrozen(ctx, tx)
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	// Shared selector lock allows the real HTTP API's per-request shared check.
	s, err := p.readStore().ShareSelection(ctx, tx)
	if err != nil {
		return projection.ReadSwitch{}, err
	}
	o, err := p.readStore().LockOperation(ctx, tx, id)
	if err != nil {
		return o, err
	}
	if err = matchCommittedRead(g, s, o); err != nil {
		return o, err
	}
	ready, err := p.checkReadReady(ctx, s, o, probe)
	if err != nil {
		return o, err
	}
	if err = executionEvidence(ctx, tx, id, "readiness", "read_switch_api_ready", ready); err != nil {
		return o, err
	}
	if err = readCAS(tx.ExecContext(ctx, `UPDATE sync_projection_read_switches SET stage='API_READY',operation_revision=operation_revision+1,last_error=NULL,updated_at=UTC_TIMESTAMP(6) WHERE operation_id=? AND stage IN ('COMMITTED','API_READY') AND operation_revision=?`, id, o.Revision)); err != nil {
		return o, err
	}
	if err = p.observe("before_api_ready_commit", ready); err != nil {
		return o, err
	}
	if err = tx.Commit(); err != nil {
		return o, fmt.Errorf("API_READY outcome unknown; inspect and live reobserve: %w", err)
	}
	if err = p.observe("after_api_ready_commit", ready); err != nil {
		return o, err
	}
	return p.readStore().Operation(ctx, id)
}
func matchCommittedRead(g projection.Gate, s projection.ReadSelection, o projection.ReadSwitch) error {
	if (o.Stage != projection.ReadCommitted && o.Stage != projection.ReadAPIReady) || g.State != "FROZEN" || g.FreezeID == nil || *g.FreezeID != o.FreezeID || g.Revision != o.FreezeRevision || s.VersionID != o.ToVersionID || s.ContractSHA256 != o.ToContractSHA256 || s.ReadRevision != o.ExpectedReadRevision+1 || s.CurrentOperationID == nil || *s.CurrentOperationID != o.ID {
		return fmt.Errorf("READ_SWITCH_STATE_CHANGED: committed selection/freeze differs")
	}
	return nil
}
func (p *ReadSwitchControl) ResumeReadSwitch(ctx context.Context, id, freezeID string, probe ReadAPIProbe) (projection.Gate, error) {
	release, err := p.lock(ctx)
	if err != nil {
		return projection.Gate{}, err
	}
	defer release()
	// Unknown COMMIT is recovered from durable facts before any live mutation.
	o, err := p.readStore().Operation(ctx, id)
	if err != nil {
		return projection.Gate{}, err
	}
	if o.Stage == projection.ReadResumed {
		s, err := p.readStore().Selection(ctx)
		if err != nil {
			return projection.Gate{}, err
		}
		g, err := projection.ReadGate(ctx, p.Store.DB)
		if err != nil || g.State != "OPEN" || g.Revision != o.FreezeRevision+1 || freezeID != o.FreezeID || s.VersionID != o.ToVersionID || s.ReadRevision != o.ExpectedReadRevision+1 {
			return g, fmt.Errorf("READ_RESUME_OUTCOME_AMBIGUOUS")
		}
		return g, nil
	}
	tx, err := p.Store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return projection.Gate{}, err
	}
	defer tx.Rollback()
	g, err := projection.LockFrozen(ctx, tx)
	if err != nil {
		return g, err
	}
	s, err := p.readStore().ShareSelection(ctx, tx)
	if err != nil {
		return g, err
	}
	o, err = p.readStore().LockOperation(ctx, tx, id)
	if err != nil {
		return g, err
	}
	if err = matchCommittedRead(g, s, o); err != nil {
		return g, err
	}
	if o.Stage != projection.ReadAPIReady || freezeID != o.FreezeID {
		return g, fmt.Errorf("READ_API_READY_REQUIRED: explicit exact freeze/ready stage required")
	}
	e, err := p.readStore().LockExecution(ctx, tx, id)
	if err != nil {
		return g, err
	}
	var historical ReadReady
	if json.Unmarshal(e.Readiness, &historical) != nil || historical.API.Identity.BootID != probe.BootID {
		return g, fmt.Errorf("READ_API_READY_REQUIRED: boot changed; reobserve API_READY first")
	}
	if _, err = p.checkReadReady(ctx, s, o, probe); err != nil {
		return g, err
	}
	pending, e2 := activeReadOperationCount(ctx, tx)
	var handoffs int
	if e2 != nil || pending != 1 {
		return g, fmt.Errorf("READ_SWITCH_STATE_CHANGED: competing operation")
	}
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_projection_handoffs WHERE phase<>'VERIFIED'").Scan(&handoffs); err != nil || handoffs != 0 {
		return g, fmt.Errorf("HANDOFF_INCOMPLETE")
	}
	if err = p.observe("before_read_resume_recheck", historical); err != nil {
		return g, err
	}
	if err = readruntime.Alive(ctx, historical.API.Identity.Process); err != nil {
		return g, err
	}
	// Upgrade the gate only after real HTTP probes (which read gate metadata)
	// have completed; selector/op locks and the original SHARE gate hold intent.
	g, err = projection.LockGateExclusive(ctx, tx)
	if err != nil {
		return g, err
	}
	if err = matchCommittedRead(g, s, o); err != nil {
		return g, err
	}
	g, err = projection.OpenReadSwitchGate(ctx, tx, freezeID)
	if err != nil {
		return g, err
	}
	if err = readCAS(tx.ExecContext(ctx, `UPDATE sync_projection_read_switches SET stage='RESUMED',operation_revision=operation_revision+1,last_error=NULL,updated_at=UTC_TIMESTAMP(6) WHERE operation_id=? AND stage='API_READY' AND operation_revision=?`, id, o.Revision)); err != nil {
		return g, err
	}
	evidence := map[string]any{"operation_id": id, "freeze_id": freezeID, "freeze_revision": o.FreezeRevision, "gate": g, "boot_id": probe.BootID, "version_id": s.VersionID, "read_revision": s.ReadRevision, "resumed_at": time.Now().UTC()}
	if err = executionEvidence(ctx, tx, id, "resumed", "read_switch_resumed", evidence); err != nil {
		return g, err
	}
	if err = p.observe("before_read_resume_commit", evidence); err != nil {
		return g, err
	}
	if err = readruntime.Alive(ctx, historical.API.Identity.Process); err != nil {
		return g, err
	}
	if err = tx.Commit(); err != nil {
		return g, fmt.Errorf("read Resume outcome unknown; inspect gate/operation before retry: %w", err)
	}
	if err = p.observe("after_read_resume_commit", evidence); err != nil {
		return g, err
	}
	return projection.ReadGate(ctx, p.Store.DB)
}

// Validate probe secrets without putting their text in parser errors.
func ProbeFromEnvironment(boot, token, permission, start, end string) (ReadAPIProbe, error) {
	p := ReadAPIProbe{BootID: boot, Token: token, PermissionToken: permission, Analytics: analytics.Request{Start: start, End: end}}
	if len(token) < 8 || len(token) > 512 || strings.TrimSpace(boot) == "" {
		return p, fmt.Errorf("API boot and probe credentials required")
	}
	_, _, err := p.Analytics.Window()
	return p, err
}

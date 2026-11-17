// Operator-only projection maintenance. Candidate handoff mutates only its new
// Kafka groups; no HTTP control route, ACTIVE promotion or alias change.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/mysqlstore"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/readruntime"
	"ticketpulse/internal/search"
	"ticketpulse/internal/syncpipe"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "TicketPulse control:", err)
		os.Exit(1)
	}
}
func run() error {
	mode := flag.String("mode", "status", "migrate, migrate-handoff, migrate-read-control, migrate-read-runtime, migrate-read-rollback, read-init, read-begin, read-validate, read-cancel, read-status, read-switch-status, read-switch-drain-plan, read-switch-drain-observe, read-switch-commit, read-switch-observe-ready, read-switch-resume, read-switch-cancel-resume, status, freeze, resume, enroll-existing, prepare, version, check, handoff-begin, handoff-pin, handoff-init, handoff-consume, handoff-verify, handoff-status")
	rollbackOf := flag.String("rollback-of", "", "exact committed read operation reversed by this intent")
	operation := flag.String("operation", "", "persistent read switch operation ID")
	direction := flag.String("direction", projection.ReadPromote, "PROMOTE or ROLLBACK intent only")
	readRevision := flag.Int64("read-revision", 0, "expected current read revision for read-begin")
	drainScope := flag.String("api-scope", "", "private same-host API executable and disabled restart inventory JSON")
	apiBoot := flag.String("api-boot", "", "current API boot ID for real readiness/resume probes")
	probeStart := flag.String("probe-start", "", "analytics proof start date")
	probeEnd := flag.String("probe-end", "", "analytics proof exclusive end date")
	handoff := flag.String("handoff", "", "durable candidate handoff ID")
	id := flag.String("version", "", "immutable target version ID")
	job := flag.String("job", "", "completed M4 both-target candidate job ID")
	reason := flag.String("reason", "", "business freeze reason")
	freezeID := flag.String("freeze-id", "", "exact durable freeze ID required to resume")
	wait := flag.Duration("wait", 30*time.Second, "finite operation timeout, >0 to 5m")
	flag.Parse()
	if *wait <= 0 || *wait > 5*time.Minute {
		return fmt.Errorf("wait must be >0 and <=5m")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *wait)
	defer cancel()
	// A separate operator DSN is intentional. Ordinary users have no control API.
	dsn := os.Getenv("TICKETPULSE_CONTROL_DSN")
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || cfg.DBName == "" {
		return fmt.Errorf("TICKETPULSE_CONTROL_DSN with source schema is required")
	}
	db, err := mysqlstore.Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	encode := func(value any) error {
		out := json.NewEncoder(os.Stdout)
		out.SetIndent("", "  ")
		return out.Encode(value)
	}
	versions := &projection.VersionStore{DB: db}
	switch *mode {
	case "migrate-read-rollback":
		if err := projection.EnsureReadRollbackSchema(ctx, db); err != nil {
			return err
		}
		return encode(map[string]any{"migration": "015_projection_read_rollback", "read_selection_changed": false})
	case "migrate-read-runtime":
		if err := projection.EnsureReadRuntimeSchema(ctx, db); err != nil {
			return err
		}
		return encode(map[string]any{"migration": "014_projection_read_runtime", "read_selection_changed": false})
	case "migrate-read-control":
		if err := projection.EnsureReadControlSchema(ctx, db); err != nil {
			return err
		}
		return encode(map[string]any{"migration": "013_projection_read_control", "initialization_performed": false, "api_read_target_changed": false})
	case "read-status", "read-switch-status", "read-cancel":
		c := &syncpipe.ReadSwitchControl{ProjectionControl: &syncpipe.ProjectionControl{Store: &syncpipe.StateStore{DB: db, Schema: cfg.DBName}}}
		if *mode == "read-status" || *mode == "read-switch-status" {
			status, err := c.ReadSwitchStatus(ctx, *operation)
			if err != nil {
				return err
			}
			return encode(status)
		}
		o, err := c.CancelPlannedReadSwitch(ctx, *operation)
		if err != nil {
			return err
		}
		return encode(o)
	case "migrate-handoff":
		if err := projection.EnsureHandoffSchema(ctx, db); err != nil {
			return err
		}
		return encode(map[string]any{"migration": "012_projection_handoff", "active_changed": false})
	case "handoff-status":
		h, err := (&projection.HandoffStore{DB: db}).Get(ctx, *handoff)
		if err != nil {
			return err
		}
		return encode(h)
	case "migrate":
		if err := projection.EnsureSchema(ctx, db); err != nil {
			return err
		}
		return encode(map[string]any{"migration": "011_projection_control", "business_data_changed": false})
	case "status":
		gate, err := projection.ReadGate(ctx, db)
		if err != nil {
			return err
		}
		return encode(gate)
	case "freeze":
		gate, err := projection.Freeze(ctx, db, *reason)
		if err != nil {
			return err
		}
		return encode(gate)
	case "resume":
		gate, err := projection.Resume(ctx, db, *freezeID)
		if err != nil {
			return err
		}
		return encode(gate)
	case "version":
		version, err := versions.Get(ctx, *id)
		if err != nil {
			return err
		}
		return encode(version)
	case "enroll-existing", "prepare", "check", "handoff-begin", "handoff-pin", "handoff-init", "handoff-consume", "handoff-verify", "read-init", "read-begin", "read-validate", "read-switch-drain-plan", "read-switch-drain-observe", "read-switch-commit", "read-switch-observe-ready", "read-switch-resume", "read-switch-cancel-resume":
	default:
		return fmt.Errorf("unsupported control mode; no ACTIVE promotion command")
	}
	var queue *syncpipe.Queue
	if strings.HasPrefix(*mode, "read-") {
		queue, err = syncpipe.ConnectStatusQueue(os.Getenv("TICKETPULSE_KAFKA_BROKERS"), cfg.DBName)
	} else {
		queue, err = syncpipe.ConnectQueue(os.Getenv("TICKETPULSE_KAFKA_BROKERS"), cfg.DBName)
	}
	if err != nil {
		return err
	}
	defer queue.Close()
	if strings.HasPrefix(*mode, "read-switch-") {
		if !projection.ValidID(*operation) {
			return fmt.Errorf("read switch command requires operation_id")
		}
		c := &syncpipe.ReadSwitchControl{ProjectionControl: &syncpipe.ProjectionControl{Store: &syncpipe.StateStore{DB: db, Schema: cfg.DBName}, Queue: queue}}
		switch *mode {
		case "read-switch-drain-plan":
			data, err := os.ReadFile(*drainScope)
			if err != nil {
				return err
			}
			var scope readruntime.Scope
			if err = json.Unmarshal(data, &scope); err != nil {
				return fmt.Errorf("invalid API inventory JSON")
			}
			e, err := c.ConfigureReadDrain(ctx, *operation, scope)
			if err != nil {
				return err
			}
			return encode(e)
		case "read-switch-drain-observe":
			e, err := c.ObserveReadDrain(ctx, *operation)
			if err != nil {
				return err
			}
			return encode(e)
		case "read-switch-commit":
			o, err := c.CommitReadSelection(ctx, *operation)
			if err != nil {
				return err
			}
			return encode(o)
		case "read-switch-observe-ready", "read-switch-resume", "read-switch-cancel-resume":
			probe, err := syncpipe.ProbeFromEnvironment(*apiBoot, os.Getenv("TICKETPULSE_READ_PROBE_TOKEN"), os.Getenv("TICKETPULSE_READ_PERMISSION_TOKEN"), *probeStart, *probeEnd)
			if err != nil {
				return err
			}
			if *mode == "read-switch-observe-ready" {
				o, err := c.ObserveAPIReady(ctx, *operation, probe)
				if err != nil {
					return err
				}
				return encode(o)
			}
			if *mode == "read-switch-cancel-resume" {
				g, err := c.ResumeCancelledReadSwitch(ctx, *operation, *freezeID, probe)
				if err != nil {
					return err
				}
				return encode(g)
			}
			g, err := c.ResumeReadSwitch(ctx, *operation, *freezeID, probe)
			if err != nil {
				return err
			}
			return encode(g)
		}
	}
	if *mode == "read-begin" || *mode == "read-validate" {
		c := &syncpipe.ReadSwitchControl{ProjectionControl: &syncpipe.ProjectionControl{Store: &syncpipe.StateStore{DB: db, Schema: cfg.DBName}, Queue: queue}}
		var o projection.ReadSwitch
		if *mode == "read-begin" {
			if *rollbackOf == "" {
				o, err = c.BeginReadSwitch(ctx, *operation, *direction, *id, *readRevision)
			} else {
				o, err = c.BeginReadSwitch(ctx, *operation, *direction, *id, *readRevision, *rollbackOf)
			}
		} else {
			o, err = c.ValidateReadSwitchDestination(ctx, *operation)
		}
		if err != nil {
			return err
		}
		return encode(o)
	}
	es, err := search.NewElasticsearch(os.Getenv("TICKETPULSE_ES_URL"), os.Getenv("TICKETPULSE_SEARCH_INDEX"))
	if err != nil {
		return err
	}
	ck, err := analytics.NewClickHouse(os.Getenv("TICKETPULSE_CK_URL"), os.Getenv("TICKETPULSE_CK_DATABASE"))
	if err != nil {
		return err
	}
	control := &syncpipe.ProjectionControl{Store: &syncpipe.StateStore{DB: db, Schema: cfg.DBName}, Queue: queue, ES: es, CK: ck}
	if *mode == "read-init" {
		s, err := (&syncpipe.ReadSwitchControl{ProjectionControl: control}).InitializeReadSelection(ctx, *id)
		if err != nil {
			return err
		}
		return encode(s)
	}
	if strings.HasPrefix(*mode, "handoff-") {
		versionID := *id
		if *mode != "handoff-begin" {
			h, err := (&projection.HandoffStore{DB: db}).Get(ctx, *handoff)
			if err != nil {
				return err
			}
			versionID = h.VersionID
		}
		// Use the persisted candidate targets, then validate live identities.
		es, ck, err := syncpipe.CandidateTargets(ctx, db, versionID)
		if err != nil {
			return err
		}
		control.ES, control.CK = es, ck
		c := &syncpipe.HandoffControl{ProjectionControl: control, CatchupTimeout: *wait}
		var h projection.Handoff
		switch *mode {
		case "handoff-begin":
			h, err = c.Begin(ctx, *handoff, versionID)
		case "handoff-pin":
			h, err = c.Pin(ctx, *handoff)
		case "handoff-init":
			h, err = c.Initialize(ctx, *handoff)
		case "handoff-consume":
			h, err = c.OpenConsumption(ctx, *handoff)
		case "handoff-verify":
			h, err = c.Verify(ctx, *handoff)
		}
		if err != nil {
			return err
		}
		return encode(h)
	}
	if *mode == "check" {
		managed, err := control.Managed(ctx, *id)
		if err != nil {
			return err
		}
		return encode(map[string]any{"state": "identity_verified", "version_id": managed.Version.ID, "contract_sha256": managed.Version.Fingerprint(), "handoff_verified": false, "freshness_verified": false})
	}
	var version projection.Version
	if *mode == "prepare" {
		version, err = control.Prepare(ctx, *id, *job)
	} else {
		version, err = control.EnrollExisting(ctx, *id)
	}
	if err != nil {
		return err
	}
	return encode(version)
}

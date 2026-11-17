package syncpipe

import (
	"context"
	"fmt"
	"strconv"
	"ticketpulse/internal/analytics"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/search"
	"time"
)

type SelectedAPI struct {
	Selection   projection.ReadSelection
	Destination *ReadDestination
	BootID      string
}

func (p *ProjectionControl) ResolveReadSelection(ctx context.Context, expectedVersion string) (*SelectedAPI, error) {
	s, err := (&projection.ReadControlStore{DB: p.Store.DB}).Selection(ctx)
	if err != nil {
		return nil, err
	}
	if expectedVersion == "" || expectedVersion != s.VersionID {
		return nil, fmt.Errorf("READ_SELECTION_CHANGED: managed expected version is required and must match selector")
	}
	d, err := p.ResolveReadDestination(ctx, s.VersionID)
	if err != nil {
		return nil, err
	}
	if d.Version.Fingerprint() != s.ContractSHA256 || (s.HandoffID == nil) != (d.Handoff == nil) || d.Handoff != nil && *s.HandoffID != d.Handoff.ID {
		return nil, fmt.Errorf("READ_SELECTION_CHANGED: selected contract/handoff differs")
	}
	if p.ES == nil || p.CK == nil || p.ES.Endpoint() != d.ES.Endpoint() || p.ES.Index() != d.ES.Index() || p.CK.Endpoint() != d.CK.Endpoint() || p.CK.Database() != d.CK.Database() {
		return nil, fmt.Errorf("%w: environment expectations do not match authoritative read selector", projection.ErrTargetMismatch)
	}
	a := &SelectedAPI{Selection: s, Destination: d}
	if err = a.CheckReadSelection(ctx); err != nil {
		return nil, err
	}
	return a, nil
}
func (a *SelectedAPI) CursorTarget() string {
	return a.Selection.VersionID + ":" + a.Selection.ContractSHA256 + ":read_revision=" + strconv.FormatInt(a.Selection.ReadRevision, 10)
}
func (a *SelectedAPI) CheckReadSelection(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := (&projection.ReadControlStore{DB: a.Destination.control.Store.DB}).CheckAPI(ctx, a.Selection, a.BootID); err != nil {
		return err
	}
	if err := a.Destination.CheckIdentities(ctx); err != nil {
		return err
	}
	uuid, err := readCKDatabaseUUID(ctx, a.Destination.CK)
	if err != nil {
		return err
	}
	if uuid != a.Destination.CKDatabaseUUID {
		return fmt.Errorf("%w: selected CK database changed", projection.ErrTargetMismatch)
	}
	if a.Selection.ReadRevision > 1 {
		o, err := (&projection.ReadControlStore{DB: a.Destination.control.Store.DB}).Operation(ctx, *a.Selection.CurrentOperationID)
		if err != nil {
			return err
		}
		if uuid != o.CKDatabaseUUID {
			return fmt.Errorf("%w: committed CK database changed", projection.ErrTargetMismatch)
		}
	}
	for _, component := range []string{"es", "ck"} {
		if _, _, err := a.Destination.RawOffsetProgress(ctx, component); err != nil {
			return err
		}
	}
	return nil
}
func (a *SelectedAPI) TargetStatus() (*TargetStatus, error) {
	if a.Destination.Handoff == nil {
		return nil, fmt.Errorf("legacy status uses original readers")
	}
	t, err := a.Destination.handoffControl().ConsumerTarget(context.Background(), a.Destination.Handoff.ID)
	if err != nil {
		return nil, err
	}
	return &TargetStatus{Target: t}, nil
}

// Concrete wrappers put selection metadata on the exact status response read
// by the service. They never synthesize candidate status from legacy progress.
type ReadFreshness struct {
	Selected *SelectedAPI
	Reader   search.FreshnessReader
}

func (r *ReadFreshness) SearchFreshness(ctx context.Context) (search.Freshness, error) {
	f, err := r.Reader.SearchFreshness(ctx)
	if err != nil {
		return f, err
	}
	f.VersionID = r.Selected.Selection.VersionID
	f.ReadRevision = r.Selected.Selection.ReadRevision
	f.ConsumerGroup = r.Selected.Destination.Groups["es"]
	f.StateNamespace = r.Selected.Destination.StateNamespace
	return f, nil
}

type ReadAnalyticsStatus struct {
	Selected *SelectedAPI
	Reader   analytics.StatusReader
}

func (r *ReadAnalyticsStatus) AnalyticsStatus(ctx context.Context) (analytics.SyncStatus, error) {
	s, err := r.Reader.AnalyticsStatus(ctx)
	if err != nil {
		return s, err
	}
	if s.ConsumerGroup != r.Selected.Destination.Groups["ck"] {
		return s, fmt.Errorf("READ_STATUS_TARGET_MISMATCH")
	}
	s.VersionID = r.Selected.Selection.VersionID
	s.ReadRevision = r.Selected.Selection.ReadRevision
	s.StateNamespace = r.Selected.Destination.StateNamespace
	return s, nil
}

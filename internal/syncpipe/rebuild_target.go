package syncpipe

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"ticketpulse/internal/analytics"
	"ticketpulse/internal/search"
)

func (r *Rebuilder) esIdentity(ctx context.Context, index string) (string, bool, error) {
	es, err := search.NewElasticsearch(r.ESURL, index)
	if err != nil {
		return "", false, err
	}
	return es.Identity(ctx)
}
func ckIdentity(ctx context.Context, ck *analytics.ClickHouse, database string) (string, bool, error) {
	if ck.Database() != database {
		return "", false, fmt.Errorf("CK identity namespace differs")
	}
	identity, exists, err := ck.Identity(ctx)
	if err != nil {
		return "", false, fmt.Errorf("REBUILD_TARGET_CHANGED: %w", err)
	}
	if !exists {
		return "", false, nil
	}
	return identity.String(), true, nil
}
func (r *Rebuilder) targets(ctx context.Context, j *RebuildJob) (*search.Elasticsearch, *analytics.ClickHouse, error) {
	var es *search.Elasticsearch
	var ck *analytics.ClickHouse
	var err error
	if j.Target != "ck" {
		es, err = search.NewElasticsearch(r.ESURL, j.Index)
		if err != nil {
			return nil, nil, err
		}
	}
	if j.Target != "es" {
		ck, err = analytics.NewClickHouse(r.CKURL, j.Database)
		if err != nil {
			return nil, nil, err
		}
	}
	if (es == nil || j.Identity["es"] != "") && (ck == nil || j.Identity["ck"] != "") {
		return es, ck, r.verifyIdentities(ctx, *j, es, ck)
	}
	identities := j.Identity
	if identities == nil {
		identities = map[string]string{}
	}
	saveIdentity := func() error {
		data, _ := json.Marshal(identities)
		_, err := r.Store.DB.ExecContext(ctx, `UPDATE sync_rebuild_jobs SET target_identity=?,updated_at=UTC_TIMESTAMP(6) WHERE job_id=?`, data, j.ID)
		j.Identity = identities
		return err
	}
	if es != nil {
		actual, exists, err := r.esIdentity(ctx, j.Index)
		if err != nil {
			return nil, nil, err
		}
		if identities["es"] != "" {
			if !exists || actual != identities["es"] {
				return nil, nil, fmt.Errorf("REBUILD_TARGET_CHANGED: ES identity differs")
			}
		} else {
			if exists {
				return nil, nil, fmt.Errorf("REBUILD_TARGET_UNCLAIMED: ES namespace already exists; inspect ownership before a new job")
			}
			if err = es.EnsureIndex(ctx); err != nil {
				return nil, nil, err
			}
			id, _, err := r.esIdentity(ctx, j.Index)
			if err != nil {
				return nil, nil, err
			}
			identities["es"] = id
			if err = saveIdentity(); err != nil {
				return nil, nil, err
			}
		}
	}
	if ck != nil {
		if identities["ck"] != "" {
			actual, exists, err := ckIdentity(ctx, ck, j.Database)
			if err != nil {
				return nil, nil, err
			}
			if !exists || actual != identities["ck"] {
				return nil, nil, fmt.Errorf("REBUILD_TARGET_CHANGED: CK identity differs")
			}
		} else {
			data, err := ck.QueryGlobal(ctx, "SELECT count() FROM system.databases WHERE name='"+j.Database+"' FORMAT TabSeparated")
			if err != nil {
				return nil, nil, err
			}
			if strings.TrimSpace(string(data)) != "0" {
				return nil, nil, fmt.Errorf("REBUILD_TARGET_UNCLAIMED: CK namespace already exists; inspect ownership before a new job")
			}
			if err = ck.Ensure(ctx); err != nil {
				return nil, nil, err
			}
			id, _, err := ckIdentity(ctx, ck, j.Database)
			if err != nil {
				return nil, nil, err
			}
			identities["ck"] = id
			if err = saveIdentity(); err != nil {
				return nil, nil, err
			}
		}
	}
	data, _ := json.Marshal(identities)
	if _, err = r.Store.DB.ExecContext(ctx, `UPDATE sync_rebuild_jobs SET target_identity=?,updated_at=UTC_TIMESTAMP(6) WHERE job_id=?`, data, j.ID); err != nil {
		return nil, nil, err
	}
	j.Identity = identities
	if err = r.log(ctx, j.ID, "snapshot", "targets_created", identities); err != nil {
		return nil, nil, err
	}
	return es, ck, nil
}
func (r *Rebuilder) verifyIdentities(ctx context.Context, j RebuildJob, es *search.Elasticsearch, ck *analytics.ClickHouse) error {
	// A long-running follower must detect a same-name topic replacement before
	// applying a fetched batch, not only when its Kafka client is opened.
	topics, err := r.Queue.Admin.ListTopics(ctx, r.Queue.Topic)
	if err != nil {
		return err
	}
	detail, ok := topics[r.Queue.Topic]
	if !ok || detail.Err != nil || detail.ID.String() != j.TopicID || len(detail.Partitions) != int(r.Queue.Partitions) {
		return fmt.Errorf("REBUILD_KAFKA_CHANGED: topic identity/partitions differ")
	}
	var uuid string
	if err := r.Store.DB.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		return err
	}
	if uuid != j.SourceUUID {
		return fmt.Errorf("REBUILD_SOURCE_CHANGED: MySQL UUID differs")
	}
	if es != nil {
		id, exists, err := r.esIdentity(ctx, j.Index)
		if err != nil {
			return err
		}
		if !exists || id != j.Identity["es"] {
			return fmt.Errorf("REBUILD_TARGET_CHANGED: ES index was removed or recreated; create a new isolated job")
		}
	}
	if ck != nil {
		id, exists, err := ckIdentity(ctx, ck, j.Database)
		if err != nil {
			return err
		}
		if !exists || id != j.Identity["ck"] {
			return fmt.Errorf("REBUILD_TARGET_CHANGED: CK tables were removed or recreated; create a new isolated job")
		}
	}
	return nil
}

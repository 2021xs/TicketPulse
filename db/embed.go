package db

import _ "embed"

//go:embed schema.sql
var Schema string

//go:embed seed.sql
var Seed string

//go:embed migrations/003_resolution_note.sql
var ResolutionNoteMigration string

//go:embed migrations/004_collaboration.sql
var CollaborationMigration string

//go:embed migrations/005_web_sessions.sql
var WebSessionsMigration string

//go:embed migrations/006_search_sync.sql
var SearchSyncMigration string

//go:embed migrations/007_analytics_sync.sql
var AnalyticsSyncMigration string

//go:embed migrations/008_kafka_sync.sql
var KafkaSyncMigration string

//go:embed migrations/009_rebuild.sql
var RebuildMigration string

//go:embed migrations/010_reconcile_index.sql
var ReconcileIndexMigration string

//go:embed migrations/011_projection_control.sql
var ProjectionControlMigration string

//go:embed migrations/012_projection_handoff.sql
var ProjectionHandoffMigration string

//go:embed migrations/013_projection_read_control.sql
var ProjectionReadControlMigration string

//go:embed migrations/014_projection_read_runtime.sql
var ProjectionReadRuntimeMigration string

//go:embed migrations/015_projection_read_rollback.sql
var ProjectionReadRollbackMigration string

//go:embed clickhouse.sql
var ClickHouseSchema string

//go:embed login_seed.sql
var LoginSeed string

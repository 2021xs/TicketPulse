"""Reviewable query candidates; these do not alter production tables or CDC."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ORIGINAL = (ROOT / 'artifacts/m5-03/sql/original/metrics.sql').read_text().strip()

FUSED = """WITH
versions AS (
 SELECT organization_id,ticket_id,version,
 any(tuple(business_event_id,action,occurred_at,event_team_id,initial_team_id,before_team_id,before_status,after_status,assignee_id,created_at,first_resolved_at)) AS fact,
 uniqExact(tuple(business_event_id,action,occurred_at,event_team_id,initial_team_id,before_team_id,before_status,after_status,assignee_id,created_at,first_resolved_at)) AS variants
 FROM ticket_events WHERE organization_id={org:UInt64}
 GROUP BY organization_id,ticket_id,version
),
firsts AS (
 SELECT ticket_id,
 argMinIf(tuple(fact.3,fact.5),version,fact.2='created') AS created,
 argMinIf(tuple(fact.3,fact.4),version,fact.2='resolved' AND fact.8='awaiting_confirmation') AS resolved,
 max(fact.2='created') AS has_created,
 max(fact.2='resolved' AND fact.8='awaiting_confirmation') AS has_resolved,max(variants) AS variants
 FROM versions GROUP BY ticket_id
),
event_rows AS (
 SELECT arrayJoin([
 tuple('created',created.1,created.2,has_created,toFloat64(0)),
 tuple('resolved',resolved.1,resolved.2,has_created AND has_resolved,dateDiff('microsecond',created.1,resolved.1)/1000000.0),
 tuple('conflict',toDateTime64(0,6,'UTC'),toUInt64(0),variants>1,toFloat64(0))
 ]) AS value FROM firsts
),
state_versions AS (
 SELECT organization_id,ticket_id,version,complete,
 any(tuple(current_team_id,assignee_id,status,initial_team_id,created_at,updated_at,first_resolved_at)) AS content,
 uniqExact(tuple(current_team_id,assignee_id,status,initial_team_id,created_at,updated_at,first_resolved_at)) AS variants
 FROM ticket_states WHERE organization_id={org:UInt64}
 GROUP BY organization_id,ticket_id,version,complete
),
latest AS (
 SELECT ticket_id,argMax(tuple(content.1,content.3),tuple(version,complete)) AS current,max(variants) AS variants
 FROM state_versions GROUP BY ticket_id
),
state_rows AS (
 SELECT arrayJoin([
 tuple('backlog',current.1,toUInt64(current.2 IN ('pending','in_progress') AND (empty({teams:Array(UInt64)}) OR has({teams:Array(UInt64)},current.1)))),
 tuple('observed',toUInt64(0),toUInt64(1)),
 tuple('conflict',toUInt64(0),toUInt64(variants>1))
 ]) AS value FROM latest
),
metrics AS (
 SELECT value.1 AS metric,if(value.1='conflict','',toString(toDate(value.2,'Asia/Shanghai'))) AS day,value.3 AS team_id,count() AS amount,sum(value.5) AS duration
 FROM event_rows WHERE value.4 AND (value.1='conflict' OR (value.2>={start:DateTime64(6,'UTC')} AND value.2<{end:DateTime64(6,'UTC')}
 AND (empty({teams:Array(UInt64)}) OR has({teams:Array(UInt64)},value.3))))
 GROUP BY metric,day,team_id
 UNION ALL
 SELECT value.1 AS metric,'' AS day,value.2 AS team_id,sum(value.3) AS amount,toFloat64(0) AS duration
 FROM state_rows WHERE value.3>0 GROUP BY metric,team_id
)
SELECT metric,day,team_id,amount,duration FROM metrics
WHERE throwIf(metric='conflict','conflicting ClickHouse business version')=0
ORDER BY metric,day,team_id
SETTINGS optimize_aggregation_in_order=1
FORMAT JSONEachRow"""

# Exact min/max comparisons use explicit NULL tags. They compare whole business
# contents without a hash, keeping NULL distinct from real zero/epoch values.
FUSED_BOUNDED = FUSED.replace(
    'any(tuple(business_event_id,action,occurred_at,event_team_id,initial_team_id,before_team_id,before_status,after_status,assignee_id,created_at,first_resolved_at)) AS fact,\n uniqExact(tuple(business_event_id,action,occurred_at,event_team_id,initial_team_id,before_team_id,before_status,after_status,assignee_id,created_at,first_resolved_at)) AS variants',
    "min(tuple(business_event_id,action,occurred_at,event_team_id,initial_team_id,tuple(isNull(before_team_id),ifNull(before_team_id,0)),before_status,after_status,tuple(isNull(assignee_id),ifNull(assignee_id,0)),created_at,tuple(isNull(first_resolved_at),ifNull(first_resolved_at,toDateTime64(0,6,'UTC'))))) AS fact,\n max(tuple(business_event_id,action,occurred_at,event_team_id,initial_team_id,tuple(isNull(before_team_id),ifNull(before_team_id,0)),before_status,after_status,tuple(isNull(assignee_id),ifNull(assignee_id,0)),created_at,tuple(isNull(first_resolved_at),ifNull(first_resolved_at,toDateTime64(0,6,'UTC'))))) != fact AS conflicting",
).replace(
    'any(tuple(current_team_id,assignee_id,status,initial_team_id,created_at,updated_at,first_resolved_at)) AS content,\n uniqExact(tuple(current_team_id,assignee_id,status,initial_team_id,created_at,updated_at,first_resolved_at)) AS variants',
    "min(tuple(current_team_id,tuple(isNull(assignee_id),ifNull(assignee_id,0)),status,initial_team_id,created_at,updated_at,tuple(isNull(first_resolved_at),ifNull(first_resolved_at,toDateTime64(0,6,'UTC'))))) AS content,\n max(tuple(current_team_id,tuple(isNull(assignee_id),ifNull(assignee_id,0)),status,initial_team_id,created_at,updated_at,tuple(isNull(first_resolved_at),ifNull(first_resolved_at,toDateTime64(0,6,'UTC'))))) != content AS conflicting",
).replace('max(variants) AS variants', 'max(conflicting) AS conflicting').replace('variants>1', 'conflicting').replace(
    'SETTINGS optimize_aggregation_in_order=1',
    'SETTINGS optimize_aggregation_in_order=1,aggregation_in_order_max_block_bytes=1048576,max_block_size=8192',
)

# Idempotent per-ticket minima/maxima operate directly on immutable business
# versions. Exact duplicate rows do not alter argMin/argMax. Equal-key conflict
# validation remains the existing full-content Reconcile responsibility.
DIRECT = """WITH
firsts AS (
 SELECT ticket_id,
 argMinIf(tuple(occurred_at,initial_team_id),version,action='created') AS created,
 argMinIf(tuple(occurred_at,event_team_id),version,action='resolved' AND after_status='awaiting_confirmation') AS resolved,
 countIf(action='created')>0 AS has_created,
 countIf(action='resolved' AND after_status='awaiting_confirmation')>0 AS has_resolved
 FROM ticket_events WHERE organization_id={org:UInt64}
 GROUP BY organization_id,ticket_id
),
event_rows AS (
 SELECT arrayJoin([
 tuple('created',created.1,created.2,has_created,toFloat64(0)),
 tuple('resolved',resolved.1,resolved.2,has_created AND has_resolved,dateDiff('microsecond',created.1,resolved.1)/1000000.0)
 ]) AS value FROM firsts
),
latest AS (
 SELECT ticket_id,argMax(tuple(current_team_id,status),tuple(version,complete)) AS current
 FROM ticket_states WHERE organization_id={org:UInt64}
 GROUP BY organization_id,ticket_id
),
state_rows AS (
 SELECT arrayJoin([
 tuple('backlog',current.1,toUInt64(current.2 IN ('pending','in_progress') AND (empty({teams:Array(UInt64)}) OR has({teams:Array(UInt64)},current.1)))),
 tuple('observed',toUInt64(0),toUInt64(1))
 ]) AS value FROM latest
),
metrics AS (
 SELECT value.1 AS metric,toString(toDate(value.2,'Asia/Shanghai')) AS day,value.3 AS team_id,count() AS amount,sum(value.5) AS duration
 FROM event_rows WHERE value.4 AND value.2>={start:DateTime64(6,'UTC')} AND value.2<{end:DateTime64(6,'UTC')}
 AND (empty({teams:Array(UInt64)}) OR has({teams:Array(UInt64)},value.3))
 GROUP BY metric,day,team_id
 UNION ALL
 SELECT value.1 AS metric,'' AS day,value.2 AS team_id,sum(value.3) AS amount,toFloat64(0) AS duration
 FROM state_rows WHERE value.3>0 GROUP BY metric,team_id
)
SELECT metric,day,team_id,amount,duration FROM metrics ORDER BY metric,day,team_id
SETTINGS optimize_aggregation_in_order=1,aggregation_in_order_max_block_bytes=1048576,max_block_size=8192
FORMAT JSONEachRow"""


DIRECT_CHECKED = DIRECT.replace(
    "countIf(action='resolved' AND after_status='awaiting_confirmation')>0 AS has_resolved",
    "countIf(action='resolved' AND after_status='awaiting_confirmation')>0 AS has_resolved,\n uniqExact(tuple(version,business_event_id,action,occurred_at,event_team_id,initial_team_id,before_team_id,before_status,after_status,assignee_id,created_at,first_resolved_at))!=uniqExact(version) AS conflicting",
).replace(
    "tuple('resolved',resolved.1,resolved.2,has_created AND has_resolved,dateDiff('microsecond',created.1,resolved.1)/1000000.0)",
    "tuple('resolved',resolved.1,resolved.2,has_created AND has_resolved,dateDiff('microsecond',created.1,resolved.1)/1000000.0),\n tuple('conflict',toDateTime64(0,6,'UTC'),toUInt64(0),conflicting,toFloat64(0))",
).replace(
    'SELECT ticket_id,argMax(tuple(current_team_id,status),tuple(version,complete)) AS current',
    'SELECT ticket_id,argMax(tuple(current_team_id,status),tuple(version,complete)) AS current,\n uniqExact(tuple(version,complete,current_team_id,assignee_id,status,initial_team_id,created_at,updated_at,first_resolved_at))!=uniqExact(tuple(version,complete)) AS conflicting',
).replace(
    "tuple('observed',toUInt64(0),toUInt64(1))",
    "tuple('observed',toUInt64(0),toUInt64(1)),\n tuple('conflict',toUInt64(0),toUInt64(conflicting))",
).replace(
    "SELECT value.1 AS metric,toString(toDate(value.2,'Asia/Shanghai')) AS day",
    "SELECT value.1 AS metric,if(value.1='conflict','',toString(toDate(value.2,'Asia/Shanghai'))) AS day",
).replace(
    "FROM event_rows WHERE value.4 AND value.2>={start:DateTime64(6,'UTC')} AND value.2<{end:DateTime64(6,'UTC')}\n AND (empty({teams:Array(UInt64)}) OR has({teams:Array(UInt64)},value.3))",
    "FROM event_rows WHERE value.4 AND (value.1='conflict' OR (value.2>={start:DateTime64(6,'UTC')} AND value.2<{end:DateTime64(6,'UTC')}\n AND (empty({teams:Array(UInt64)}) OR has({teams:Array(UInt64)},value.3))))",
).replace(
    'SELECT metric,day,team_id,amount,duration FROM metrics ORDER BY metric,day,team_id',
    "SELECT metric,day,team_id,amount,duration FROM metrics WHERE throwIf(metric='conflict','conflicting ClickHouse business version')=0 ORDER BY metric,day,team_id",
)


def single_metric(name):
    prefix = ORIGINAL.split('metrics AS (')[0].rstrip(',\n')
    selects = {
        'backlog': "SELECT current.1 AS team_id,count() AS amount FROM latest WHERE current.2 IN ('pending','in_progress') AND (empty({teams:Array(UInt64)}) OR has({teams:Array(UInt64)},current.1)) GROUP BY team_id",
        'created': "SELECT toString(toDate(created.1,'Asia/Shanghai')) AS day,created.2 AS team_id,count() AS amount FROM creations WHERE created.1>={start:DateTime64(6,'UTC')} AND created.1<{end:DateTime64(6,'UTC')} AND (empty({teams:Array(UInt64)}) OR has({teams:Array(UInt64)},created.2)) GROUP BY day,team_id",
        'resolved': "SELECT toString(toDate(resolved.1,'Asia/Shanghai')) AS day,resolved.2 AS team_id,count() AS amount FROM resolutions WHERE resolved.1>={start:DateTime64(6,'UTC')} AND resolved.1<{end:DateTime64(6,'UTC')} AND (empty({teams:Array(UInt64)}) OR has({teams:Array(UInt64)},resolved.2)) GROUP BY day,team_id",
        'duration': "SELECT toString(toDate(r.resolved.1,'Asia/Shanghai')) AS day,r.resolved.2 AS team_id,sum(dateDiff('microsecond',c.created.1,r.resolved.1))/1000000.0/count() AS average_seconds FROM resolutions r INNER JOIN creations c ON c.ticket_id=r.ticket_id WHERE r.resolved.1>={start:DateTime64(6,'UTC')} AND r.resolved.1<{end:DateTime64(6,'UTC')} AND (empty({teams:Array(UInt64)}) OR has({teams:Array(UInt64)},r.resolved.2)) GROUP BY day,team_id",
    }
    return prefix + '\n' + selects[name] + ' FORMAT JSONEachRow'


def query(name):
    if name == 'original':
        return ORIGINAL
    if name == 'ordered':
        return ORIGINAL.replace(' FORMAT JSONEachRow', ' SETTINGS optimize_aggregation_in_order=1 FORMAT JSONEachRow')
    if name == 'ordered-bounded':
        return ORIGINAL.replace(' FORMAT JSONEachRow', ' SETTINGS optimize_aggregation_in_order=1,aggregation_in_order_max_block_bytes=1048576,max_block_size=8192 FORMAT JSONEachRow')
    if name == 'fused':
        return FUSED
    if name == 'fused-bounded':
        return FUSED_BOUNDED
    if name == 'materialized':
        return ORIGINAL.replace('events AS (', 'events AS MATERIALIZED (').replace('creations AS (', 'creations AS MATERIALIZED (').replace('latest AS (', 'latest AS MATERIALIZED (').replace(' FORMAT JSONEachRow', ' SETTINGS enable_materialized_cte=1,optimize_aggregation_in_order=1,aggregation_in_order_max_block_bytes=1048576,max_block_size=8192 FORMAT JSONEachRow')
    if name == 'direct':
        return DIRECT
    if name == 'direct-checked':
        return DIRECT_CHECKED
    if name == 'direct-small':
        return DIRECT.replace('aggregation_in_order_max_block_bytes=1048576,max_block_size=8192', 'aggregation_in_order_max_block_bytes=262144,max_block_size=2048')
    if name == 'checked-small':
        return DIRECT_CHECKED.replace('aggregation_in_order_max_block_bytes=1048576,max_block_size=8192', 'aggregation_in_order_max_block_bytes=262144,max_block_size=2048')
    if name == 'checked-bufferless':
        return DIRECT_CHECKED.replace('aggregation_in_order_max_block_bytes=1048576,max_block_size=8192', 'aggregation_in_order_max_block_bytes=131072,max_block_size=1024,read_in_order_use_buffering=0')
    if name == 'checked-one-thread':
        return DIRECT_CHECKED.replace('aggregation_in_order_max_block_bytes=1048576,max_block_size=8192', 'aggregation_in_order_max_block_bytes=131072,max_block_size=1024,read_in_order_use_buffering=0,max_threads=1,max_final_threads=1')
    if name == 'selected':
        src = (ROOT / 'internal/analytics/clickhouse.go').read_text()
        return src.split('const Canonical = ' + chr(96))[1].split(chr(96))[0]
    return single_metric(name)

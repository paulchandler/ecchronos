# core

The `core` module is the heart of ecChronos. It contains both the interfaces and their concrete implementations for distributed repair scheduling across a Cassandra cluster.

## Package Structure

```
com.ericsson.bss.cassandra.ecchronos.core
├── scheduling/         # Scheduler loop, job queue, lock and run-policy interfaces
├── repair/             # Repair job types, tasks, lock resources, configuration
│   ├── state/          # Vnode repair state, history, replication topology
│   └── types/          # View/data-transfer types for the REST API
├── metrics/            # Micrometer gauge publication per table
├── utils/              # Shared utilities (token ranges, node resolution, logging)
│   └── logging/        # Rate-limited logging helpers
└── exceptions/         # Exception hierarchy
```

---

## Scheduling Infrastructure (`scheduling/`)

### `ScheduleManagerImpl`
The central scheduling loop. Runs on a single-threaded `ScheduledExecutorService` with a configurable polling interval (default ~30 seconds). On each tick it:

1. Iterates a priority-ordered `ScheduledJobQueue`
2. Consults every registered `RunPolicy` to check whether each job is allowed to run
3. For the highest-priority eligible job, iterates its tasks
4. Acquires a distributed lock via `LockFactory.tryLock()` before executing each task
5. Calls `job.postExecute(success, task)` after the task finishes

### `ScheduledJobQueue`
A priority queue backed by a `TreeSet` using `DefaultJobComparator`. Jobs whose last successful repair is furthest in the past float to the top (most overdue = highest priority).

### `ScheduledJob` (abstract)
Base class for all jobs. Holds priority, last-successful-run timestamp, and backoff logic. Subclasses implement `iterator()` to return the list of `ScheduledTask`s to execute.

### `LockFactory` (interface)
Abstraction over distributed lock acquisition. The production implementation is `CASLockFactory` (see below).

### `RunPolicy` (interface)
Abstraction over scheduling policy enforcement. Returns the remaining blocked duration in milliseconds, or `-1` if the job is allowed to run. `TimeBasedRunPolicy` is the built-in implementation.

---

## Repair Jobs (`repair/`)

### `RepairSchedulerImpl`
Factory and lifecycle manager for `ScheduledRepairJob` instances. When a table's repair configuration is added, changed, or removed it:

- Diffs the existing job set against the new configuration
- Tears down removed jobs cleanly
- Creates `TableRepairJob` (vnode/sub-range) or `IncrementalRepairJob` based on `RepairType`
- Schedules the new job via `ScheduleManager`

### `TableRepairJob`
The default repair job for a single table. On each scheduling tick it:

- Refreshes its `RepairState` (reads history, calculates outstanding vnodes)
- Computes the job's health status: `COMPLETED`, `ON_TIME`, `LATE`, `OVERDUE`, or `BLOCKED`
- Exposes one `ScheduledTask` per outstanding `ReplicaRepairGroup`

### `IncrementalRepairJob`
Variant of `TableRepairJob` for incremental repair. Instead of querying ecChronos' own history table it relies on Cassandra's native `maxRepairedAt` / `percentRepaired` JMX metrics.

### `ScheduledRepairJob` (abstract)
Shared behaviour for `TableRepairJob` and `IncrementalRepairJob`: alarm raising/clearing via `AlarmPostUpdateHook`, view-model construction, and repair-configuration access.

### `VnodeRepairTask`
Executes a single repair session for one `ReplicaRepairGroup` (a set of token ranges sharing the same replica set). It:

1. Calls `JmxProxy.repairAsync()` on the local Cassandra node
2. Listens for JMX `RepairNotification` events
3. Writes a `RepairEntry` to `ecchronos.repair_history` on completion or failure

### `IncrementalRepairTask`
Same as `VnodeRepairTask` but for incremental repair. Does not write repair history — Cassandra tracks state internally.

### `OnDemandRepairJob` / `VnodeOnDemandRepairJob` / `IncrementalOnDemandRepairJob`
Single-run jobs triggered via the REST API. They auto-deschedule after all tasks complete. Status is persisted in `ecchronos.on_demand_repair_status` so progress survives restarts.

### `OnDemandRepairSchedulerImpl`
Manages the set of currently active on-demand jobs. Reads persisted `OngoingJob` records from Cassandra at startup to resume any in-flight repairs.

### `RepairLockFactoryImpl`
Wraps `CASLockFactory` and selects the correct lock resource scope (datacenter-level or vnode-level) based on `RepairLockType`.

### `RepairResourceFactory` implementations
Determine which Cassandra resources must be locked before a repair runs, controlling parallelism to avoid cluster overload:

| Class | Scope |
|---|---|
| `VnodeRepairResourceFactory` | One resource per vnode token range |
| `DataCenterRepairResourceFactory` | One resource per datacenter |
| `CombinedRepairResourceFactory` | Both datacenter and vnode scopes |

### `RepairConfiguration`
Value object holding all repair parameters for a table: interval, warning/error thresholds, parallelism mode, sub-range target size, repair type, backoff, and initial delay.

---

## Repair State (`repair/state/`)

This sub-package tracks whether each vnode (token range) of a table has been repaired recently enough.

### `RepairStateImpl`
Holds an `AtomicReference<RepairStateSnapshot>`. On `update()`:

1. Calls `VnodeRepairStateFactory.calculateNewState()` to read fresh state
2. Converts raw vnode states into `ReplicaRepairGroup`s (groups of vnodes sharing the same replicas)
3. Atomically swaps the snapshot
4. Updates Micrometer metrics and fires registered `PostUpdateHook` callbacks

### `RepairStateSnapshot`
Immutable view of a table's repair state at a point in time. Contains:

- `lastCompletedAt` — when the whole table was last fully repaired
- `vnodeRepairStates` — per-vnode repair timestamps
- `repairGroups` — repair work still outstanding, ready for execution
- `estimatedRepairTime` — projected time to complete all outstanding repairs

### `VnodeRepairStateFactoryImpl`
Calculates current vnode repair states by:

1. Querying `RepairHistoryProvider` for recent repair entries covering each token range
2. Cross-referencing with current ring topology from `ReplicationState`
3. Applying sub-range splitting logic if `repair.size.target` is configured

### `EccRepairHistory`
Read/write interface to `ecchronos.repair_history`. Writes repair start and end records; exposes `RepairEntry` iterators per token range.

### `RepairHistoryProviderImpl`
Cassandra-backed implementation of `RepairHistoryProvider`. Uses a Caffeine cache keyed by `(keyspace, table)` with a configurable TTL to avoid excessive CQL queries.

### `ReplicationStateImpl`
Reads the Cassandra ring topology (token ownership, replica assignments) from the Java Driver metadata. Used to determine which nodes hold replicas for each vnode.

### `ReplicaRepairGroupFactory`
Groups `VnodeRepairState`s that share the same replica set so they can be repaired together in one JMX call, reducing per-repair overhead.

### `SubRangeRepairStates`
When sub-range repair is enabled, splits large vnodes into smaller token sub-ranges based on estimated data size obtained from `TableStorageStates`.

### `AlarmPostUpdateHook`
`PostUpdateHook` implementation that raises or clears fault-manager alarms when a table transitions to `LATE` or `OVERDUE` status.

---

## Distributed Locking

### `CASLock`
Implements a distributed lease using Cassandra Lightweight Transactions (CAS) on the `ecchronos.lock` table. Key behaviour:

- **Acquire** — inserts a row with a TTL; fails if a higher-priority holder already owns the lock
- **Priority tracking** — writes to `ecchronos.lock_priority` so all nodes can see each other's relative priorities and the highest-priority node wins
- **Renewal** — a background thread renews the TTL every 60 seconds during long-running repairs
- **Release** — deletes both the lock row and the corresponding priority row

### `CASLockFactory` / `CASLockFactoryBuilder`
Creates `CASLock` instances. Manages shared Cassandra prepared statements and a `LockCache` that short-circuits repeated acquisition attempts on the same resource within a single tick.

---

## Time-Based Run Policy

### `TimeBasedRunPolicy`
Implements `RunPolicy`. Reads time-window blackout configuration from `ecchronos.reject_configuration` (cached for 10 seconds via Caffeine) and returns the remaining blocked duration, or `-1` if the repair is permitted to proceed now.

---

## Cassandra / JMX Integration

### `JmxProxyFactoryImpl` / `JmxProxy`
Opens JMX connections to local Cassandra nodes. Exposes `repairAsync()`, `getLiveNodes()`, storage metrics, and other Cassandra MBean operations needed by the scheduler.

### `HostStatesImpl`
Periodically polls `JmxProxy` to track which Cassandra nodes are `UP` or `DOWN`. Used by `RepairState` to skip repairs when required replicas are unavailable.

### `TableStorageStatesImpl`
Reads estimated table sizes via the Cassandra `StorageService` MBean. Used by sub-range repair to decide how to split large vnodes.

### `CassandraMetrics`
Caches Cassandra JMX metrics (e.g. `percentRepaired`, `maxRepairedAt`) with a Caffeine cache. Consumed by `IncrementalRepairJob` to determine repair progress without querying history.

---

## Metrics (`metrics/`)

### `TableRepairMetricsImpl`
Publishes Micrometer gauges for each managed table:

| Metric | Description |
|---|---|
| `repaired_ratio` | Fraction of vnodes repaired within the configured interval |
| `last_repaired_time_seconds` | Age of the oldest unrepaired vnode |
| `repair_sessions` | Counter of completed repair sessions |

`TableGauges` holds the per-table gauge registrations and deregisters them when a table is removed from management.

---

## Utilities (`utils/`)

| Class | Purpose |
|---|---|
| `TableReference` / `TableReferenceFactoryImpl` | Canonical `(keyspace, table)` identity object used as a map key throughout the codebase |
| `LongTokenRange` | Represents a Cassandra token range as a pair of `long` values; correctly handles token ring wrap-around |
| `NodeResolverImpl` | Resolves node `UUID` → `DriverNode` using Java Driver metadata |
| `ReplicatedTableProviderImpl` | Lists all tables that have replication configured (filters out system tables) |
| `RepairStatsProviderImpl` | Aggregates per-table repair statistics for the REST API |
| `TokenSubRangeUtil` | Splits a `LongTokenRange` into a given number of equal sub-ranges |
| `UnitConverter` | Converts between time units (used for configuration parsing) |
| `ThrottlingLogger` | Rate-limits repeated log lines to avoid flooding logs during sustained failures |

---

## Cassandra Tables Used

| Table | Purpose |
|---|---|
| `ecchronos.lock` | Distributed lease rows (with TTL) |
| `ecchronos.lock_priority` | Per-node priority records for contested locks |
| `ecchronos.repair_history` | Audit trail of all repair sessions |
| `ecchronos.on_demand_repair_status` | Persisted state of in-flight on-demand repairs |
| `ecchronos.reject_configuration` | Time-based blackout windows read by `TimeBasedRunPolicy` |

---

## Key Design Patterns

- **Builder pattern** — used extensively for complex construction (`ScheduleManagerImpl`, `RepairSchedulerImpl`, `RepairConfiguration`, etc.)
- **Factory pattern** — `RepairStateFactory`, `CASLockFactory`, `RepairResourceFactory`
- **Strategy pattern** — `RunPolicy`, `TableRepairPolicy`, `RepairResourceFactory` variants are interchangeable
- **Observer / hook pattern** — `PostUpdateHook` callbacks decouple state updates from alarm/metric side-effects
- **Atomic snapshot** — `RepairStateImpl` uses `AtomicReference<RepairStateSnapshot>` for lock-free, consistent state reads
- **CAS-based distributed lock** — priority-aware lease acquisition in Cassandra prevents concurrent repairs of the same resource across the cluster

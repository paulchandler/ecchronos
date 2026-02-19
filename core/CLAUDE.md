# core module — guidance for Claude Code

## Interface/Implementation convention

Every public API in this module must be defined as an interface first. Concrete classes use the `*Impl` suffix (e.g. `RepairState` / `RepairStateImpl`). Do not add a concrete class as the sole entry point for a new piece of functionality — create the interface first.

## Thread-safety rules

- **`ScheduleManagerImpl` is single-threaded.** All job execution happens on one `ScheduledExecutorService` thread. Tasks must never block this thread — no long sleeps, no synchronous CQL calls, no waiting on external I/O from inside a `ScheduledTask.execute()`.
- **`RepairStateImpl` uses `AtomicReference<RepairStateSnapshot>` for lock-free state reads.** The snapshot object itself must remain fully immutable after construction — never add mutable fields to `RepairStateSnapshot`.
- **`RepairSchedulerImpl` guards its job map with `synchronized` blocks.** Keep any work done inside those blocks minimal.

## Testing patterns

There are two distinct test styles in this module — use the right one:

### Pure unit tests (the majority)
Use Mockito to mock all Cassandra/JMX dependencies. No real Cassandra required. These are fast and should cover the bulk of logic.

### Container tests (heavier)
Extend `AbstractCassandraContainerTest`, which spins up a real single-node Cassandra via Testcontainers. Use this only when the test must exercise actual CQL/LWT behaviour (e.g. `TestCASLockFactory`, `TestEccRepairHistory`, `TestRepairHistoryProviderImpl`, `TestTimeBasedRunPolicy`).

The Cassandra version is controlled by the `-Dit.cassandra.version` system property (defaults to `latest`).

### Shared test helpers
Before writing new test setup code, check whether these helpers already cover your needs:

| Class | Purpose |
|---|---|
| `AbstractCassandraContainerTest` | Base class for container tests; manages cluster lifecycle |
| `DummyJob` | Minimal `ScheduledJob` implementation for scheduler tests |
| `DummyLock` | No-op `LockFactory.DistributedLock` for tests that don't need real locking |
| `MockTableReferenceFactory` | Pre-wired `TableReferenceFactory` mock |
| `TestUtils` | Common assertion and builder helpers |
| `TokenUtil` | Token range construction utilities |
| `RowUtil` | CQL `Row` stubs for history/state tests |

### Value objects — EqualsVerifier
Any new value object with a custom `equals`/`hashCode` must be verified using `EqualsVerifier`. See `TestRepairEntry` or `TestVnodeRepairState` for examples.

## CAS lock TTL

The distributed lock TTL is **10 minutes**, renewed by a background thread every **60 seconds**. Do not perform long-blocking work inside a locked section — if the renewal thread is starved of CPU the lock will expire and another node may acquire it mid-repair.

## Caffeine cache TTLs

Several classes cache CQL query results using Caffeine:

- `RepairHistoryProviderImpl` — caches repair history per `(keyspace, table)`
- `TimeBasedRunPolicy` — caches blackout windows (10-second TTL)
- `CassandraMetrics` — caches incremental repair JMX metrics

These TTLs are intentional to limit CQL query rates. Do not remove or significantly shorten them without understanding the resulting query load on the cluster.

## Adding a new repair type

If adding a new scheduled repair variant, you will need to:

1. Create a new `ScheduledRepairJob` subclass (alongside `TableRepairJob` / `IncrementalRepairJob`)
2. Create a corresponding `RepairTask` subclass (alongside `VnodeRepairTask` / `IncrementalRepairTask`)
3. Add a new `RepairType` enum value in `RepairConfiguration`
4. Update `RepairSchedulerImpl` to instantiate your new job type when the config specifies it
5. Add a new `OnDemandRepairJob` subclass if on-demand support is needed
6. Add unit tests using Mockito and, if LWT/CQL behaviour is involved, a container test extending `AbstractCassandraContainerTest`

# pgxevents

PostgreSQL `LISTEN`/`NOTIFY`-backed event propagation for Go, built on [jackc/pgx](https://github.com/jackc/pgx).

Mutations on configured tables fire events **atomically with the transaction commit**. Subscribers receive typed, fully-populated row snapshots without providing a fetcher.

```go
listener, err := pgxevents.NewListener(ctx, pool)
sub, err := pgxevents.ListenTyped[PolicyData](listener, "policy_data")
for ev := range sub.Events() {
    // ev.Action  ∈ {INSERT, UPDATE, DELETE}
    // ev.Row     *PolicyData populated from the row at commit time
    // ev.Meta    OutboxID, CreatedAt
}
```

## Why pgxevents

PostgreSQL `NOTIFY` payloads are capped at 8000 bytes — enough for a small ID, never enough to carry a full row safely. pgxevents solves this with an **ephemeral outbox**: per-table `AFTER` triggers serialize the row, INSERT it into the `pgxevents_outbox` table, and `pg_notify` carries only the outbox row's UUID. Listeners subscribe to the notification channel, fetch the snapshot by ID, and fan out typed events.

Because everything happens inside the mutation's transaction:

- **Delivery is atomic with commit.** A successful `NOTIFY` reaches listeners iff the transaction commits.
- **Payload reflects state at commit**, not state at delivery — no coalescing of rapid updates.
- **No app-side `pg_notify` calls.** Triggers ensure events fire on every write through any path (app code, migrations, ad-hoc SQL).

The outbox is `UNLOGGED` by default; durability across PG crashes isn't required because a crashed listener reconnects and recovers via subscriber-side reconciliation.

## Installation

```
go get github.com/fastforgeinc/pgxevents/pkg/pgxevents@latest
```

## Quick start

### 1. Install the DDL via migrations (recommended)

The library exports the canonical SQL as `InstallSQL()` and `TableTriggerSQL(table)`. In your `golang-migrate` (or equivalent) migrations:

```go
// migrations/0001_pgxevents_install.up.sql — generated from pgxevents.InstallSQL()
```

`InstallSQL()` is idempotent (uses `CREATE ... IF NOT EXISTS` / `CREATE OR REPLACE`) and creates:

- `pgxevents_outbox` (UNLOGGED table) + index on `created_at`
- `pgxevents_notify_event()` PL/pgSQL function with a version stamp in its `COMMENT`

For each table you want to propagate, add the trigger:

```go
// migrations/0002_subscribe_policy_data.up.sql
//   = pgxevents.TableTriggerSQL("policy_data")
```

This installs `pgxevents_<table>_trigger` `AFTER INSERT OR UPDATE OR DELETE`.

### 2. Subscribe in your service

```go
import "github.com/fastforgeinc/pgxevents/pkg/pgxevents"

pool, _ := pgxpool.New(ctx, dsn)

listener, err := pgxevents.NewListener(ctx, pool)
if err != nil { /* DDL missing / version mismatch — error includes copy-paste SQL */ }
defer listener.Close()

sub, err := pgxevents.ListenTyped[PolicyData](listener, "policy_data")
if err != nil { /* trigger missing — error includes the trigger SQL to add */ }

for ev := range sub.Events() {
    handle(ev)
}
```

`NewListener` validates that the installed DDL matches the running library `Version` via `pg_catalog`. Mismatches fail fast with an actionable error.

### Dev / test shortcut

For setups without migrations, opt in to runtime install:

```go
listener, err := pgxevents.NewListener(ctx, pool, pgxevents.WithRuntimeInstall(true))
```

This runs `InstallSQL()` and the per-table trigger SQL at startup. Requires DDL privileges on the database user.

A runnable example lives in [`examples/basic`](examples/basic/main.go).

## Configuration

All options have sensible defaults; see `options.go` for full documentation.

| Option | Default | Description |
|---|---|---|
| `WithRuntimeInstall(bool)` | `false` | Install DDL at startup instead of expecting it from migrations. |
| `WithLoggedOutbox()` | unset | Use a WAL-logged outbox table instead of UNLOGGED. Replicated to standbys; survives PG crashes. |
| `WithBackoff(BackoffConfig)` | `500ms`/`30s`/`0.2` jitter | Reconnect backoff when the LISTEN connection drops. |
| `WithSubscriberBuffer(int)` | `64` | Per-subscriber channel buffer size. |
| `WithOverflowPolicy(...)` | `OverflowDropOldest` | Slow-subscriber handling: drop oldest event or close the subscription. |
| `WithCleanupTTL(d)` | `1h` | How long outbox rows live before the cleanup loop deletes them. |
| `WithCleanupInterval(d)` | `5m` | How often the cleanup loop runs. |
| `WithLogger(Logger)` | `NoopLogger` | Adapter to your logger (zap, slog, etc.). |
| `WithMetrics(Metrics)` | `NoopMetrics` | Adapter to your metrics registry. |

## Health signal & reconnection

`Listener.Health() <-chan error` emits `nil` when the listener is established (and again on each recovery), and a non-nil error when the LISTEN connection drops. Consumers typically use it to drain their own downstream streams when the listener is unavailable, forcing reconnection-based reconciliation:

```go
go func() {
    for err := range listener.Health() {
        if err != nil {
            // Listener is down — drain downstream consumers so they re-fetch
            // initial state once we reconnect.
            myStreamPool.DrainAll()
        }
    }
}()
```

The library reconnects automatically with exponential backoff and jitter.

## Tradeoffs

- **At-most-once delivery.** PG `LISTEN`/`NOTIFY` doesn't replay missed messages. If a listener's connection drops after a NOTIFY but before delivery, that event is lost. The recommended pattern is to use the `Health()` signal to drain subscribers on failure and have them reconcile from a fresh snapshot on reconnect.
- **One LISTEN connection per process.** The listener holds one pool connection for the lifetime of the listener. Size your pool accordingly.
- **JSON unmarshal in `ListenTyped`.** Decode errors are logged + counted as `decode_error` and skipped. Keep your Go struct in sync with the table schema.
- **Outbox writes amplify mutation cost.** Every triggered mutation writes one outbox row and fires one NOTIFY. UNLOGGED storage keeps this cheap, but for very high-throughput tables consider whether eventing is really wanted on every change.

## Cleanup

A background ticker in the listener acquires a Postgres advisory lock and trims outbox rows older than `cleanupTTL`. Across multiple listener pods, only one performs the DELETE per tick. No external cron, no `pg_cron` dependency.

## License

MIT. See [LICENSE](LICENSE).

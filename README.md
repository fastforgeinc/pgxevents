# pgxevents

PostgreSQL LISTEN/NOTIFY-backed event propagation for Go, built on [jackc/pgx](https://github.com/jackc/pgx).

Mutations on configured tables fire events atomically with the transaction commit. Subscribers receive typed, fully-populated row snapshots without providing a fetcher.

## Design

- **Trigger-based auto-propagation.** A PL/pgSQL function and per-table `AFTER INSERT OR UPDATE OR DELETE` triggers capture row snapshots into a dedicated outbox table inside the mutation's transaction. Callers don't need to remember to call `pg_notify` — the schema guarantees it.
- **Ephemeral outbox.** Because PostgreSQL `NOTIFY` has an 8000-byte payload limit, each mutation writes a snapshot row to `pgxevents_outbox` (UNLOGGED by default) and `pg_notify` carries only the outbox UUID. Listeners fetch the snapshot by ID.
- **Atomic with commit.** Delivery is guaranteed iff the transaction commits. Payload reflects row state at commit, not at delivery — no coalescing of rapid updates.
- **Typed subscriptions via generics.** `pgxevents.ListenTyped[PolicyData](listener, "policy_data")` returns fully-populated `*PolicyData` events.
- **Self-managed cleanup.** A background ticker in the listener acquires a Postgres advisory lock and trims outbox rows older than a configurable TTL. No external cron or `pg_cron` dependency.

## Status

v1 is in development on `feat/v1-scaffold`. API is under design; see `pkg/pgxevents/` for the current shape. Not ready for use.

## License

MIT. See [LICENSE](LICENSE).

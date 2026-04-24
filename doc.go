// Package pgxevents provides PostgreSQL LISTEN/NOTIFY-backed event
// propagation on top of jackc/pgx/v5.
//
// # Overview
//
// Mutations on configured tables fire events atomically with the mutating
// transaction's commit. Subscribers receive typed, fully-populated row
// snapshots with no fetcher callback: the library captures the row state
// into a dedicated outbox table inside the trigger, then pg_notify carries
// the outbox UUID so listeners fetch the exact snapshot rather than the
// possibly-updated live row.
//
// # Design
//
// PostgreSQL's NOTIFY payload is capped at 8000 bytes, which is too small
// to carry arbitrary row snapshots. pgxevents solves this with an
// "ephemeral outbox": per-table AFTER triggers serialize the row, INSERT
// it into the pgxevents_outbox table, and pg_notify carries only the
// outbox row's UUID. Listeners subscribe to the notification channel,
// SELECT the snapshot by ID, and fan out typed events to subscribers.
//
// Because everything happens inside the mutation's transaction, delivery
// is atomic with commit and the snapshot reflects row state at commit
// time (not at delivery time). UNLOGGED outbox storage keeps the write
// cost low; durability across crashes is not required because a crashed
// PostgreSQL kills LISTEN sessions and subscribers reconcile via their
// own reconnection paths.
//
// # Usage
//
// Install the required DDL via golang-migrate migrations (recommended):
//
//	// In a migration:
//	//   pgxevents.InstallSQL()          — function + outbox table + index
//	//   pgxevents.TableTriggerSQL(tbl)  — per-table trigger
//
// Or pass WithRuntimeInstall(true) for dev/test setups where migrations
// are not in use.
//
// Subscribe with a typed row struct:
//
//	listener, err := pgxevents.NewListener(ctx, pool)
//	if err != nil {
//	    return err
//	}
//	defer listener.Close()
//
//	sub, err := pgxevents.ListenTyped[PolicyData](listener, "policy_data")
//	if err != nil {
//	    return err
//	}
//	for ev := range sub.Events() {
//	    // ev.Action is ActionInsert / ActionUpdate / ActionDelete.
//	    // ev.Row is *PolicyData populated from the snapshot.
//	}
package pgxevents

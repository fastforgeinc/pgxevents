package pgxevents

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// expectedFunctionComment is the COMMENT value stamped on the trigger
// function by InstallSQL. Mismatches indicate version drift between the
// installed DDL and the running library.
func expectedFunctionComment() string { return "pgxevents v" + Version }

// validateInstallBase verifies the base DDL (function + outbox table) is
// installed and matches the library Version.
func validateInstallBase(ctx context.Context, pool *pgxpool.Pool) error {
	expected := expectedFunctionComment()

	var comment *string
	err := pool.QueryRow(ctx, `
		SELECT obj_description(p.oid, 'pg_proc')
		FROM pg_proc p
		WHERE p.proname = 'pgxevents_notify_event'
		LIMIT 1
	`).Scan(&comment)
	if errors.Is(err, pgx.ErrNoRows) {
		return missingDDLError("trigger function pgxevents_notify_event not found", InstallSQL())
	}
	if err != nil {
		return fmt.Errorf("pgxevents: query trigger function: %w", err)
	}
	got := "<no comment>"
	if comment != nil {
		got = *comment
	}
	if got != expected {
		return fmt.Errorf(
			"pgxevents: trigger function version mismatch: have %q, expected %q; "+
				"reinstall via migrations or pass WithRuntimeInstall(true)\n\n%s",
			got, expected, InstallSQL(),
		)
	}

	var exists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relname = 'pgxevents_outbox'
			  AND n.nspname = current_schema()
		)
	`).Scan(&exists)
	if err != nil {
		return fmt.Errorf("pgxevents: query outbox table: %w", err)
	}
	if !exists {
		return missingDDLError("outbox table pgxevents_outbox not found", InstallSQL())
	}
	return nil
}

// validateTableTrigger verifies the per-table trigger is installed.
func validateTableTrigger(ctx context.Context, pool *pgxpool.Pool, table string) error {
	triggerName := "pgxevents_" + table + "_trigger"
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_trigger t
			JOIN pg_class c ON c.oid = t.tgrelid
			WHERE c.relname = $1
			  AND t.tgname = $2
			  AND NOT t.tgisinternal
		)
	`, table, triggerName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("pgxevents: query trigger %q on %q: %w", triggerName, table, err)
	}
	if !exists {
		return fmt.Errorf(
			"pgxevents: trigger %q not found on table %q; "+
				"add to your migration or pass WithRuntimeInstall(true):\n\n%s",
			triggerName, table, TableTriggerSQL(table),
		)
	}
	return nil
}

func missingDDLError(what, sql string) error {
	return fmt.Errorf(
		"pgxevents: %s; include InstallSQL() in your migrations or "+
			"pass WithRuntimeInstall(true):\n\n%s",
		what, sql,
	)
}

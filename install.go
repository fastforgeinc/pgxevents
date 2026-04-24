package pgxevents

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// runtimeInstallBase executes the canonical InstallSQL on pool. When
// cfg.loggedOutbox is true, the UNLOGGED keyword is stripped so the
// outbox table is created as a regular WAL-logged table.
//
// Idempotent: safe to call against a database where the DDL is already
// present.
func runtimeInstallBase(ctx context.Context, pool *pgxpool.Pool, cfg config) error {
	sql := InstallSQL()
	if cfg.loggedOutbox {
		// Replace exactly one occurrence to avoid touching unrelated text.
		sql = strings.Replace(sql, "CREATE UNLOGGED TABLE", "CREATE TABLE", 1)
	}
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("execute install SQL: %w", err)
	}
	return nil
}

// runtimeInstallTable installs the per-table AFTER trigger on table.
// Idempotent: CREATE OR REPLACE TRIGGER replaces any existing trigger
// with the same name.
func runtimeInstallTable(ctx context.Context, pool *pgxpool.Pool, table string) error {
	if _, err := pool.Exec(ctx, TableTriggerSQL(table)); err != nil {
		return fmt.Errorf("execute table trigger SQL: %w", err)
	}
	return nil
}

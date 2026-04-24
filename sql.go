package pgxevents

import (
	_ "embed"
	"fmt"
)

// Version is the library version stamped into the installed trigger
// function. NewListener validates at startup that the installed function
// matches this version; mismatches fail fast with an actionable error.
const Version = "1.0.0"

// NotifyChannel is the PostgreSQL NOTIFY channel used by the library.
const NotifyChannel = "pgxevents_event"

//go:embed sql/install.sql
var installSQL string

// InstallSQL returns the canonical SQL for installing the pgxevents
// trigger function and outbox table. Include this in a golang-migrate
// migration in services that use the default (MigrationsRequired) mode.
//
// The returned SQL is idempotent: it uses CREATE ... IF NOT EXISTS for
// the outbox table and index, and CREATE OR REPLACE for the trigger
// function, so re-running it is safe.
func InstallSQL() string { return installSQL }

// TableTriggerSQL returns the SQL for installing the per-table AFTER
// INSERT OR UPDATE OR DELETE trigger on the given table. Include this
// in a migration alongside (or after) the target table's creation.
//
// The trigger is named pgxevents_<table>_trigger and replaces any
// existing trigger with the same name.
func TableTriggerSQL(table string) string {
	return fmt.Sprintf(
		`CREATE OR REPLACE TRIGGER pgxevents_%[1]s_trigger
  AFTER INSERT OR UPDATE OR DELETE ON %[1]s
  FOR EACH ROW EXECUTE FUNCTION pgxevents_notify_event();
`, table)
}

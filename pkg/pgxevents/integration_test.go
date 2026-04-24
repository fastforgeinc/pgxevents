//go:build integration

package pgxevents

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupPostgres starts a fresh PostgreSQL container and returns a pgxpool
// connected to it. Container and pool are torn down via t.Cleanup.
func setupPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("pgxevents_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		tc.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

// uniqueTable returns a test-unique lowercase table name based on t.Name().
func uniqueTable(t *testing.T) string {
	name := strings.ToLower(t.Name())
	name = strings.ReplaceAll(name, "/", "_")
	return name
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func installAll(t *testing.T, pool *pgxpool.Pool, table string) {
	t.Helper()
	mustExec(t, pool, InstallSQL())
	mustExec(t, pool, TableTriggerSQL(table))
}

func makePolicyTable(t *testing.T, pool *pgxpool.Pool, name string) {
	t.Helper()
	mustExec(t, pool, fmt.Sprintf(`
		CREATE TABLE %s (
			id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			name text NOT NULL,
			seat int NOT NULL
		)
	`, name))
}

type testRow struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Seat int    `json:"seat"`
}

func waitTyped[T any](t *testing.T, sub TypedSubscription[T], timeout time.Duration) TypedEvent[T] {
	t.Helper()
	select {
	case ev, ok := <-sub.Events():
		if !ok {
			t.Fatalf("subscription channel closed unexpectedly")
		}
		return ev
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for event after %v", timeout)
		return TypedEvent[T]{}
	}
}

func TestIntegrationTriggerFiresOnInsertUpdateDelete(t *testing.T) {
	pool := setupPostgres(t)
	table := uniqueTable(t)
	makePolicyTable(t, pool, table)
	installAll(t, pool, table)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	listener, err := NewListener(ctx, pool)
	if err != nil {
		t.Fatalf("NewListener: %v", err)
	}
	defer listener.Close()

	sub, err := ListenTyped[testRow](listener, table)
	if err != nil {
		t.Fatalf("ListenTyped: %v", err)
	}

	mustExec(t, pool, fmt.Sprintf(`INSERT INTO %s (name, seat) VALUES ('alice', 3)`, table))
	ev := waitTyped(t, sub, 5*time.Second)
	if ev.Action != ActionInsert {
		t.Errorf("action = %v, want INSERT", ev.Action)
	}
	if ev.Row.Name != "alice" || ev.Row.Seat != 3 {
		t.Errorf("row = %+v, want alice/3", *ev.Row)
	}
	if ev.Meta.OutboxID == "" {
		t.Error("meta.OutboxID empty")
	}

	mustExec(t, pool, fmt.Sprintf(`UPDATE %s SET seat = 5 WHERE name = 'alice'`, table))
	ev = waitTyped(t, sub, 5*time.Second)
	if ev.Action != ActionUpdate {
		t.Errorf("action = %v, want UPDATE", ev.Action)
	}
	if ev.Row.Seat != 5 {
		t.Errorf("seat = %d, want 5 after update", ev.Row.Seat)
	}

	mustExec(t, pool, fmt.Sprintf(`DELETE FROM %s WHERE name = 'alice'`, table))
	ev = waitTyped(t, sub, 5*time.Second)
	if ev.Action != ActionDelete {
		t.Errorf("action = %v, want DELETE", ev.Action)
	}
	if ev.Row.Name != "alice" {
		t.Errorf("delete event should carry pre-delete row, got %+v", *ev.Row)
	}
}

func TestIntegrationLargeRowDeliveredViaOutbox(t *testing.T) {
	pool := setupPostgres(t)
	table := uniqueTable(t)
	mustExec(t, pool, fmt.Sprintf(`
		CREATE TABLE %s (
			id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			payload jsonb NOT NULL
		)
	`, table))
	installAll(t, pool, table)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	listener, err := NewListener(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	type largeRow struct {
		ID      string         `json:"id"`
		Payload map[string]any `json:"payload"`
	}
	sub, err := ListenTyped[largeRow](listener, table)
	if err != nil {
		t.Fatal(err)
	}

	bigStr := strings.Repeat("x", 20000) // exceeds NOTIFY 8000-byte limit
	bigJSON, err := json.Marshal(map[string]string{"big": bigStr})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, pool, fmt.Sprintf(`INSERT INTO %s (payload) VALUES ($1::jsonb)`, table), string(bigJSON))

	ev := waitTyped(t, sub, 5*time.Second)
	if ev.Action != ActionInsert {
		t.Errorf("action = %v", ev.Action)
	}
	got, _ := ev.Row.Payload["big"].(string)
	if len(got) != len(bigStr) {
		t.Errorf("large payload length = %d, want %d", len(got), len(bigStr))
	}
	if got != bigStr {
		t.Error("large payload content mismatch")
	}
}

func TestIntegrationValidateMissingFunction(t *testing.T) {
	pool := setupPostgres(t)
	ctx := context.Background()

	_, err := NewListener(ctx, pool)
	if err == nil {
		t.Fatal("expected error on fresh DB without DDL")
	}
	if !strings.Contains(err.Error(), "pgxevents_notify_event") {
		t.Errorf("error should mention missing function: %v", err)
	}
	if !strings.Contains(err.Error(), "WithRuntimeInstall") {
		t.Errorf("error should suggest WithRuntimeInstall: %v", err)
	}
}

func TestIntegrationValidateMissingTrigger(t *testing.T) {
	pool := setupPostgres(t)
	mustExec(t, pool, InstallSQL()) // base DDL only

	table := uniqueTable(t)
	makePolicyTable(t, pool, table)

	ctx := context.Background()
	listener, err := NewListener(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	if _, err = listener.Listen(table); err == nil {
		t.Fatal("expected error for table without trigger")
	} else if !strings.Contains(err.Error(), "pgxevents_"+table+"_trigger") {
		t.Errorf("error should mention missing trigger: %v", err)
	}
}

func TestIntegrationRuntimeInstallBootstraps(t *testing.T) {
	pool := setupPostgres(t)
	table := uniqueTable(t)
	makePolicyTable(t, pool, table)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	listener, err := NewListener(ctx, pool, WithRuntimeInstall(true))
	if err != nil {
		t.Fatalf("NewListener with WithRuntimeInstall(true): %v", err)
	}
	defer listener.Close()

	sub, err := ListenTyped[testRow](listener, table)
	if err != nil {
		t.Fatal(err)
	}

	mustExec(t, pool, fmt.Sprintf(`INSERT INTO %s (name, seat) VALUES ('bob', 1)`, table))
	ev := waitTyped(t, sub, 5*time.Second)
	if ev.Row.Name != "bob" {
		t.Errorf("got %+v, want bob/1", *ev.Row)
	}
}

func TestIntegrationHealthSignalOnStartup(t *testing.T) {
	pool := setupPostgres(t)
	table := uniqueTable(t)
	makePolicyTable(t, pool, table)
	installAll(t, pool, table)

	listener, err := NewListener(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	select {
	case h := <-listener.Health():
		if h != nil {
			t.Errorf("expected nil (healthy) startup signal, got %v", h)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("listener did not emit health signal within 5s")
	}
}

func TestIntegrationCleanupDeletesExpiredRows(t *testing.T) {
	pool := setupPostgres(t)
	table := uniqueTable(t)
	makePolicyTable(t, pool, table)
	installAll(t, pool, table)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	listener, err := NewListener(ctx, pool,
		WithCleanupTTL(100*time.Millisecond),
		WithCleanupInterval(150*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	sub, err := ListenTyped[testRow](listener, table)
	if err != nil {
		t.Fatal(err)
	}

	mustExec(t, pool, fmt.Sprintf(`INSERT INTO %s (name, seat) VALUES ('temp', 0)`, table))
	_ = waitTyped(t, sub, 5*time.Second)

	// Wait long enough for at least one cleanup tick after the TTL elapses.
	time.Sleep(800 * time.Millisecond)

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pgxevents_outbox`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("expected outbox empty after cleanup, got %d rows", n)
	}
}

func TestIntegrationOverflowDisconnect(t *testing.T) {
	pool := setupPostgres(t)
	table := uniqueTable(t)
	makePolicyTable(t, pool, table)
	installAll(t, pool, table)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	listener, err := NewListener(ctx, pool,
		WithSubscriberBuffer(2),
		WithOverflowPolicy(OverflowDisconnect),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	// Untyped Listen so we can hold the channel without consuming it; the
	// typed wrapper would forward eagerly via its decoder goroutine.
	sub, err := listener.Listen(table)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 8; i++ {
		mustExec(t, pool, fmt.Sprintf(`INSERT INTO %s (name, seat) VALUES ($1, $2)`, table),
			fmt.Sprintf("row%d", i), i)
	}

	// Let the listener process the burst into the (size-2) buffer and trip
	// OverflowDisconnect on event 3+ before we start draining. Reading
	// concurrently with fanout would prevent the buffer from ever filling.
	time.Sleep(1 * time.Second)

	seen := 0
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-sub.Events():
			if !ok {
				if seen > 2 {
					t.Errorf("expected at most 2 buffered events before disconnect, saw %d", seen)
				}
				return
			}
			seen++
		case <-deadline:
			t.Fatalf("subscription did not close after overflow (saw %d events)", seen)
		}
	}
}

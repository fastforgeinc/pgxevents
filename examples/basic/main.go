// Basic pgxevents example: subscribe to row changes on a "notes" table
// and print each event as it arrives. Inserts a new row every second so
// the example produces output without any external action.
//
// Usage:
//
//	export DATABASE_URL=postgres://user:pass@localhost:5432/db?sslmode=disable
//	go run ./examples/basic
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fastforgeinc/pgxevents"
)

// Note matches the demo table schema.
type Note struct {
	ID   string `json:"id"`
	Body string `json:"body"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS notes (
			id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			body text NOT NULL
		)
	`); err != nil {
		log.Fatalf("create demo table: %v", err)
	}

	// Runtime install for the demo so it works without migrations.
	// Production code should ship the DDL via golang-migrate and use the
	// default MigrationsRequired mode.
	listener, err := pgxevents.NewListener(ctx, pool,
		pgxevents.WithRuntimeInstall(true),
	)
	if err != nil {
		log.Fatalf("new listener: %v", err)
	}
	defer func() { _ = listener.Close() }()

	sub, err := pgxevents.ListenTyped[Note](listener, "notes")
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	go func() {
		for ev := range sub.Events() {
			fmt.Printf("[%s] note %s: %s\n", ev.Action, ev.Row.ID, ev.Row.Body)
		}
	}()

	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				if _, err := pool.Exec(ctx,
					`INSERT INTO notes (body) VALUES ($1)`,
					"hello at "+now.Format(time.RFC3339),
				); err != nil {
					log.Printf("insert: %v", err)
				}
			}
		}
	}()

	<-ctx.Done()
}

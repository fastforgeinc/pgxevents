package main

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ypopivniak/pgxevents/pkg/pgxevents"
	"log"
	"os"
	"os/signal"
	"time"
)

type Record struct {
	Id   int
	Text string
}

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		log.Fatal(err)
	}

	_, err = pool.Exec(ctx, "CREATE TEMP TABLE IF NOT EXISTS pgxevents (id INTEGER, text VARCHAR)")
	if err != nil {
		log.Fatal(err)
	}

	l, err := pgxevents.NewListener(ctx, pool, pgxevents.WithMaxRetries(10))
	if err != nil {
		log.Fatal(err)
	}

	ch, err := l.Listen("pgxevents")
	if err != nil {
		log.Fatal(err)
	}

	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
Loop:
	for {
		select {
		case event, ok := <-ch:
			if !ok {
				log.Print("listener is closed")
				break Loop
			}
			payload := new(Record)
			if err = event.Scan(payload); err != nil {
				log.Fatalf("failed scan event %+v into %T", event, payload)
			}
			log.Printf("action %s, payload %+v", event.Action, payload)
		case <-t.C:
			_, err = pool.Exec(ctx, "INSERT INTO pgxevents VALUES(floor(random() * 10 + 1)::int, 'hello world')")
			if err != nil {
				log.Print(err)
			}
		}
	}
}

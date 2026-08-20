package pgx5

import (
	"context"
	"os"
	"testing"

	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMigrate(t *testing.T) {
	p := &Postgres{}
	addr := os.Getenv("POSTGRES_PORT")
	if addr == "" {
		addr = "5432"
	}
	dsn := "postgres://postgres@localhost:" + addr + "/postgres?sslmode=disable"
	dt.Test(t, p, []byte(dsn))
}

func TestWithInstance_InitializationFailureConnectionLeak(t *testing.T) {
	addr := os.Getenv("POSTGRES_PORT")
	if addr == "" {
		addr = "5432"
	}
	dsn := "postgres://postgres@localhost:" + addr + "/postgres?sslmode=disable"

	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Trigger a failure by using an invalid table name
	config := &Config{
		MigrationsTable: "invalid table name with spaces",
	}

	_, err = WithInstance(db, config)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Verify that the number of acquired connections returns to 0
	acquiredConns := db.Stat().AcquiredConns()
	if acquiredConns != 0 {
		t.Errorf("expected 0 acquired connections, got %d", acquiredConns)
	}
}
package pgx5

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func init() {
	database.Register("pgx5", &Postgres{})
}

var (
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
	ErrAppendConfig   = fmt.Errorf("can't append configuration")
)

type Config struct {
	MigrationsTable  string
	SchemaName       string
	StatementTimeout time.Duration
}

func (c *Config) validate() error { 
	if len(c.MigrationsTable) == 0 {
		c.MigrationsTable = "schema_migrations"
	}
	return nil
}

type Postgres struct {
	// Boilerplate
	isDestroyed bool

	// Config
	config *Config

	// Connections
	db   *pgxpool.Pool
	conn *pgxpool.Conn
}

func WithInstance(db *pgxpool.Pool, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := config.validate(); err != nil {
		return nil, err
	}

	conn, err := db.Acquire(context.Background())
	if err != nil {
		return nil, err
	}

	px := &Postgres{
		conn:   conn,
		db:     db,
		config: config,
	}

	if err := px.EnsureVersionTable(); err != nil {
		conn.Release()
		return nil, err
	}

	return px, nil
}

func (p *Postgres) Open(url string) (database.Driver, error) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, err
	}

	px := &Postgres{
		conn:   conn,
		db:     pool,
		config: &Config{},
	}

	if err := px.EnsureVersionTable(); err != nil {
		conn.Release()
		pool.Close()
		return nil, err
	}

	return px, nil
}

func (p *Postgres) Close() error {
	p.conn.Release()
	p.db.Close()
	return nil
}

// Lock is an implementation of database.Driver.Lock.
func (p *Postgres) Lock() error {
	if p.isDestroyed {
		return database.ErrLocked
	}

	aid, err := database.GenerateAdvisoryLockId(p.config.MigrationsTable)
	if err != nil {
		return err
	}

	query := "SELECT pg_advisory_lock($1)"
	if _, err := p.conn.Exec(context.Background(), query, aid); err != nil {
		return err
	}

	return nil
}

// Unlock is an implementation of database.Driver.Unlock.
func (p *Postgres) Unlock() error {
	if p.isDestroyed {
		return database.ErrLocked
	}

	aid, err := database.GenerateAdvisoryLockId(p.config.MigrationsTable)
	if err != nil {
		return err
	}

	query := "SELECT pg_advisory_unlock($1)"
	if _, err := p.conn.Exec(context.Background(), query, aid); err != nil {
		return err
	}

	return nil
}

// Run is an implementation of database.Driver.Run.
func (p *Postgres) Run(migration io.Reader) error {
	if p.isDestroyed {
		return database.ErrLocked
	}

	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	query := string(migr)
	if _, err := p.conn.Exec(context.Background(), query); err != nil {
		return err
	}

	return nil
}

// SetVersion is an implementation of database.Driver.SetVersion.
func (p *Postgres) SetVersion(version int, dirty bool) error {
	if p.isDestroyed {
		return database.ErrLocked
	}

	tx, err := p.conn.Begin(context.Background())
	if err != nil {
		return err
	}

	query := `TRUNCATE ` + p.config.MigrationsTable
	if _, err := tx.Exec(context.Background(), query); err != nil {
		tx.Rollback(context.Background())
		return err
	}

	if version >= 0 {
		query = `INSERT INTO ` + p.config.MigrationsTable + ` (version, dirty) VALUES ($1, $2)`
		if _, err := tx.Exec(context.Background(), query, version, dirty); err != nil {
			tx.Rollback(context.Background())
			return err
		}
	}

	if err := tx.Commit(context.Background()); err != nil {
		return err
	}

	return nil
}

// Version is an implementation of database.Driver.Version.
func (p *Postgres) Version() (version int, dirty bool, err error) {
	if p.isDestroyed {
		return 0, false, database.ErrLocked
	}

	query := `SELECT version, dirty FROM ` + p.config.MigrationsTable + ` LIMIT 1`
	err = p.conn.QueryRow(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == pgx.ErrNoRows:
		return database.NilVersion, false, nil
	case err != nil:
		return 0, false, err
	default:
		return version, dirty, nil
	}
}

// Drop is an implementation of database.Driver.Drop.
func (p *Postgres) Drop() error {
	if p.isDestroyed {
		return database.ErrLocked
	}

	// select all tables in current schema
	query := `SELECT table_name FROM information_schema.tables WHERE table_schema = CURRENT_SCHEMA AND table_type = 'BASE TABLE'`
	rows, err := p.conn.Query(context.Background(), query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return err
		}
		tables = append(tables, table)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if len(tables) > 0 {
		query = `DROP TABLE IF EXISTS ` + strings.Join(tables, ", ") + ` CASCADE`
		if _, err := p.conn.Exec(context.Background(), query); err != nil {
			return err
		}
	}

	return nil
}

// EnsureVersionTable is an implementation of database.Driver.EnsureVersionTable.
func (p *Postgres) EnsureVersionTable() (err error) {
	if p.isDestroyed {
		return database.ErrLocked
	}

	if err = p.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := p.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = fmt.Errorf("%s: %s", err.Error(), e.Error())
			}
		}
	}()

	if len(p.config.SchemaName) > 0 {
		// set search path
		query := `SET search_path TO ` + p.config.SchemaName
		if _, err := p.conn.Exec(context.Background(), query); err != nil {
			return err
		}
	}

	// create migration table if not exists
	query := `CREATE TABLE IF NOT EXISTS ` + p.config.MigrationsTable + ` (version bigint NOT NULL PRIMARY KEY, dirty boolean NOT NULL)`
	if _, err := p.conn.Exec(context.Background(), query); err != nil {
		return err
	}

	return nil
}
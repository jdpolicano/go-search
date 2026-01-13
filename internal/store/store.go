package store

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

/**
 * Interface that joins pgx.Conn and pgx.Tx for easier handling of transactions.
 * A caller can just pass in either a pgx.Conn or pgx.Tx where a DBTX is expected.
 */
type DBTX interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Store struct {
	Pool *pgxpool.Pool
}

func NewStore(dbPath string) (Store, error) {
	ctx := context.Background()
	pool, openErr := pgxpool.New(ctx, "user=postgres dbname=gosearch host=/tmp")
	if openErr != nil {
		return Store{}, openErr
	}
	return Store{pool}, nil
}

package postgres

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed 001_initial.sql
var sqlFS embed.FS

func RunMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	b, err := sqlFS.ReadFile("001_initial.sql")
	if err != nil {
		return fmt.Errorf("read migration: %w", err)
	}
	_, err = pool.Exec(ctx, string(b))
	return err
}

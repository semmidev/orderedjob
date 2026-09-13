package sqlserver

import (
	"context"
	"fmt"
)

func (r *Repository) Migrate(ctx context.Context) error {
	b, err := migrationFS.ReadFile("001_initial.sql")
	if err != nil {
		return fmt.Errorf("read migration file: %w", err)
	}
	if _, err := r.db.ExecContext(ctx, string(b)); err != nil {
		return fmt.Errorf("execute migration: %w", err)
	}
	return nil
}

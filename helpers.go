package sqlhelp

import (
	"context"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

// In this file:  some generic helper function that have functions that might
// suit the most common datasets, i.e. those that have an "id" column as a
// primary key.

// SelectRowByID selects a row by ID (assuming that ID column is named "id").
func SelectRowByID[T any](ctx context.Context, db sqlx.ExtContext, table string, id int64) (*T, error) {
	return SelectRow[T](ctx, db, table, sq.Eq{"id": id})
}

// SelectRowByIntegrationID selects a row by integration_id (assuming that
// there is an "integration_id" column).
func SelectRowByIntegrationID[T any](ctx context.Context, db sqlx.ExtContext, table string, integrationID string) (*T, error) {
	return SelectRow[T](ctx, db, table, sq.Eq{"integration_id": integrationID})
}

func DeleteByID(ctx context.Context, db sqlx.ExtContext, table string, id any) error {
	return Delete(ctx, db, table, sq.Eq{"id": id})
}

// UpdateByID updates a record by ID.
func UpdateByID[T any](ctx context.Context, db sqlx.ExtContext, table string, id any, a *T) (int64, error) {
	return Update(ctx, db, table, a, sq.Eq{"id": id})
}

// ExistsByID checks if a record with the given ID exists.
func ExistsByID(ctx context.Context, db sqlx.ExtContext, table string, id any) (bool, error) {
	return Exists(ctx, db, table, sq.Eq{"id": id})
}

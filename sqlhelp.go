// package sqlhelp provides a set of generic helper functions to work with SQL
// databases.
package sqlhelp

import (
	"context"
	"database/sql"
	"errors"
	"iter"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/rusq/tagops"
)

var Tag = "db"

// Insert is a generic function to insert a record into a table.
func Insert[T any](ctx context.Context, db sqlx.ExtContext, table string, a T) (int64, error) {
	return InsertFull(ctx, db, true, table, a)
}

// InsertFull is a generic function to insert a record into a table, if
// omitEmpty is specified, fields with empty values will be omitted from the
// insert statement.
func InsertFull[T any](ctx context.Context, db sqlx.ExtContext, omitEmpty bool, table string, a T) (int64, error) {
	bld := sq.Insert(table).SetMap(tagops.ToMap(a, Tag, omitEmpty, true)).Suffix("ON CONFLICT DO NOTHING")
	stmt, binds, err := bld.ToSql()
	if err != nil {
		return 0, err
	}

	res, err := db.ExecContext(ctx, db.Rebind(stmt), binds...)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return id, err
	}
	return id, nil
}

// InsertPSQL is a Postgres flavour of Insert.
func InsertPSQL[T any](ctx context.Context, db sqlx.ExtContext, table string, idCol string, a T) (int64, error) {
	return InsertPSQLFull(ctx, db, true, table, idCol, a)
}

// InsertPSQLFull is a Postgres flavour of InsertFull.
func InsertPSQLFull[T any](ctx context.Context, db sqlx.ExtContext, omitEmpty bool, table string, idCol string, a T) (int64, error) {
	bld := sq.Insert(table).SetMap(tagops.ToMap(a, Tag, omitEmpty, false)).Suffix("ON CONFLICT DO NOTHING RETURNING " + idCol)
	stmt, binds, err := bld.ToSql()
	if err != nil {
		return 0, err
	}

	var id int64
	if err := db.QueryRowxContext(ctx, db.Rebind(stmt), binds...).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

// SelectRow selects a row from a table.
func SelectRow[T any](ctx context.Context, db sqlx.ExtContext, table string, where sq.Sqlizer) (*T, error) {
	var res T
	bld := sq.Select(tagops.Tags(&res, Tag)...).From(table).Where(where)
	query, args, err := bld.ToSql()
	if err != nil {
		return nil, err
	}
	if err := db.QueryRowxContext(ctx, db.Rebind(query), args...).StructScan(&res); err != nil {
		return nil, err
	}
	return &res, nil
}

// Update updates a record.
func Update[T any](ctx context.Context, db sqlx.ExtContext, table string, a *T, where sq.Sqlizer) (int64, error) {
	bld := sq.Update(table).SetMap(tagops.ToMap(a, Tag, true, false)).Where(where)
	query, args, err := bld.ToSql()
	if err != nil {
		return 0, err
	}
	res, err := db.ExecContext(ctx, db.Rebind(query), args...)
	if err != nil {
		return 0, err
	}
	raff, err := res.RowsAffected()
	if err != nil {
		return raff, err
	}
	return raff, err
}

// JustErr is a helper function to return just an error from a function that
// returns two values, where the first one is not needed and the second is an
// error.
func JustErr(_ any, err error) error {
	return err
}

// Delete deletes rows from the table matching where argument.
func Delete(ctx context.Context, db sqlx.ExtContext, table string, where sq.Sqlizer) error {
	bld := sq.Delete(table).Where(where)
	query, args, err := bld.ToSql()
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, db.Rebind(query), args...)
	if err != nil {
		return err
	}
	return nil
}

// Select selects rows from a table.
func Select[T any](ctx context.Context, db sqlx.ExtContext, table string, where sq.Sqlizer) (iter.Seq2[T, error], error) {
	var t T
	bld := sq.Select(tagops.Tags(&t, Tag)...).From(table).Where(where)
	query, args, err := bld.ToSql()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryxContext(ctx, db.Rebind(query), args...)
	if err != nil {
		return nil, err
	}
	iterFunc := func(yield func(T, error) bool) {
		defer rows.Close()
		for rows.Next() {
			var t T
			err := rows.StructScan(&t)
			if !yield(t, err) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			var t2 T
			yield(t2, err)
		}
	}
	return iterFunc, nil
}

// Collect2 collects all values from the iterator into a slice.
func Collect2[T any](seq iter.Seq2[T, error]) ([]T, error) {
	var res []T
	for t, err := range seq {
		if err != nil {
			return nil, err
		}
		res = append(res, t)
	}
	return res, nil
}

func Exists(ctx context.Context, db sqlx.ExtContext, table string, where sq.Sqlizer) (bool, error) {
	bld := sq.Select("1 as X").From(table).Where(where)
	query, args, err := bld.ToSql()
	if err != nil {
		return false, err
	}
	var exists int64
	if err := db.QueryRowxContext(ctx, db.Rebind(query), args...).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return exists == 1, nil
}

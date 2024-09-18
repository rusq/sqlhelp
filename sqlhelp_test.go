// package sqlhelp provides a set of generic helper functions to work with SQL
// databases.
package sqlhelp

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/rusq/sqlhelp/sqlhelptest"
	"github.com/stretchr/testify/assert"
	_ "modernc.org/sqlite"
)

type TestStruct struct {
	Bool      bool      `db:"bool_t,omitempty"`
	CreatedAt time.Time `db:"created_at,omitempty"`
	ID        int       `db:"id"`
	Int       int       `db:"int_t,omitempty"`
	Name      string    `db:"name"`
	Nested
}

type Nested struct {
	Street    string `db:"street,omitempty"`
	NestedInt int    `db:"nested_int,omitmepty"`
}

var (
	testDate = time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC)

	filledStruct = TestStruct{
		Bool:      true,
		CreatedAt: testDate,
		ID:        1,
		Int:       2,
		Name:      "test",
		Nested: Nested{
			Street:    "street",
			NestedInt: 3,
		},
	}
	testStructCols   = []string{"bool_t", "created_at", "id", "int_t", "name", "nested_int", "street"}
	testStructBinds  = []driver.Value{true, testDate, 1, 2, "test", 3, "street"}
	testStructSelect = `SELECT ` + strings.Join(testStructCols, ", ") + ` FROM test_table WHERE id = \$1`
	testStructInsert = `INSERT INTO test_table \(` + strings.Join(testStructCols, ",") + `\) VALUES`
	testStructUpdate = `UPDATE test_table SET bool_t = \$1, created_at = \$2, id = \$3, int_t = \$4, name = \$5, nested_int = \$6, street = \$7 WHERE id = \$8`
	testStructDelete = `DELETE FROM test_table WHERE id = \$1`
)

func TestInsert(t *testing.T) {
	type args[T any] struct {
		ctx context.Context
		// db    sqlx.ExtContext // provided by runner
		table string
		a     T
	}
	tests := []struct {
		name     string
		args     args[TestStruct]
		expectFn func(mock sqlmock.Sqlmock)
		want     int64
		wantErr  bool
	}{
		{
			"ok",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				a:     filledStruct,
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(testStructInsert).
					WithArgs(testStructBinds...).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			1,
			false,
		},
		{
			"exec error",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				a:     filledStruct,
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(testStructInsert).
					WithArgs(testStructBinds...).
					WillReturnError(assert.AnError)
			},
			0,
			true,
		},
		{
			"last insert id error",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				a:     filledStruct,
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(testStructInsert).
					WithArgs(testStructBinds...).
					WillReturnResult(sqlmock.NewErrorResult(assert.AnError))
			},
			0,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock := sqlhelptest.InitMockDB(t)
			tt.expectFn(mock)
			got, err := Insert(tt.args.ctx, db, tt.args.table, tt.args.a)
			if (err != nil) != tt.wantErr {
				t.Errorf("Insert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Insert() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInsertPSQL(t *testing.T) {
	type args[T any] struct {
		ctx context.Context
		// db    sqlx.ExtContext
		table string
		idCol string
		a     T
	}
	tests := []struct {
		name     string
		args     args[TestStruct]
		expectFn func(mock sqlmock.Sqlmock)
		want     int64
		wantErr  bool
	}{
		{
			"ok",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				idCol: "id",
				a:     filledStruct,
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(testStructInsert).
					WithArgs(testStructBinds...).
					WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1)).
					RowsWillBeClosed()
			},
			1,
			false,
		},
		{
			"query error",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				idCol: "id",
				a:     filledStruct,
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(testStructInsert).
					WithArgs(testStructBinds...).
					WillReturnError(assert.AnError)
			},
			0,
			true,
		},
		{
			"last insert id error",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				idCol: "id",
				a:     filledStruct,
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(testStructInsert).
					WithArgs(testStructBinds...).
					WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1).RowError(0, assert.AnError))
			},
			0,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock := sqlhelptest.InitMockDB(t)
			tt.expectFn(mock)
			got, err := InsertPSQL(tt.args.ctx, db, tt.args.table, tt.args.idCol, tt.args.a)
			if (err != nil) != tt.wantErr {
				t.Errorf("InsertPSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("InsertPSQL() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSelectRow(t *testing.T) {
	type args[T any] struct {
		ctx context.Context
		// db    sqlx.ExtContext
		table string
		where sq.Sqlizer
	}
	tests := []struct {
		name     string
		args     args[TestStruct]
		expectFn func(mock sqlmock.Sqlmock)
		want     *TestStruct
		wantErr  bool
	}{
		{
			"ok",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				where: sq.Eq{"id": 1},
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(testStructSelect).
					WithArgs(1).
					WillReturnRows(sqlmock.NewRows(testStructCols).AddRow(testStructBinds...))
			},
			&filledStruct,
			false,
		},
		{
			"query error",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				where: sq.Eq{"id": 1},
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(testStructSelect).
					WithArgs(1).
					WillReturnError(assert.AnError)
			},
			nil,
			true,
		},
		{
			"scan error",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				where: sq.Eq{"id": 1},
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(testStructSelect).
					WithArgs(1).
					WillReturnRows(sqlmock.NewRows(testStructCols).AddRow(testStructBinds...).RowError(0, assert.AnError))
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock := sqlhelptest.InitMockDB(t)
			tt.expectFn(mock)
			got, err := SelectRow[TestStruct](tt.args.ctx, db, tt.args.table, tt.args.where)
			if (err != nil) != tt.wantErr {
				t.Errorf("SelectRow() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SelectRow() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	type args[T any] struct {
		ctx context.Context
		// db    sqlx.ExtContext
		table string
		a     *T
		where sq.Eq
	}
	tests := []struct {
		name     string
		args     args[TestStruct]
		expectFn func(mock sqlmock.Sqlmock)
		want     int64
		wantErr  bool
	}{
		{
			"ok",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				a:     &filledStruct,
				where: sq.Eq{"id": 1},
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(testStructUpdate).
					WithArgs(append(testStructBinds, 1)...).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			1,
			false,
		},
		{
			"exec error",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				a:     &filledStruct,
				where: sq.Eq{"id": 1},
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(testStructUpdate).
					WithArgs(append(testStructBinds, 1)...).
					WillReturnError(assert.AnError)
			},
			0,
			true,
		},
		{
			"rows affected error",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				a:     &filledStruct,
				where: sq.Eq{"id": 1},
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(testStructUpdate).
					WithArgs(append(testStructBinds, 1)...).
					WillReturnResult(sqlmock.NewErrorResult(assert.AnError))
			},
			0,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock := sqlhelptest.InitMockDB(t)
			tt.expectFn(mock)

			got, err := Update(tt.args.ctx, db, tt.args.table, tt.args.a, tt.args.where)
			if (err != nil) != tt.wantErr {
				t.Errorf("Update() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Update() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJustErr(t *testing.T) {
	type args struct {
		in0 any
		err error
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"no error",
			args{
				in0: TestStruct{},
				err: nil,
			},
			false,
		},
		{
			"error",
			args{
				in0: TestStruct{},
				err: assert.AnError,
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := JustErr(tt.args.in0, tt.args.err); (err != nil) != tt.wantErr {
				t.Errorf("JustErr() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	type args struct {
		ctx context.Context
		// db    sqlx.ExtContext
		table string
		where sq.Eq
	}
	tests := []struct {
		name     string
		args     args
		expectFn func(mock sqlmock.Sqlmock)
		wantErr  bool
	}{
		{
			"ok",
			args{
				ctx:   context.Background(),
				table: "test_table",
				where: sq.Eq{"id": 1},
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(testStructDelete).
					WithArgs(1).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			false,
		},
		{
			"exec error",
			args{
				ctx:   context.Background(),
				table: "test_table",
				where: sq.Eq{"id": 1},
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(testStructDelete).
					WithArgs(1).
					WillReturnError(assert.AnError)
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock := sqlhelptest.InitMockDB(t)
			tt.expectFn(mock)
			if err := Delete(tt.args.ctx, db, tt.args.table, tt.args.where); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSelect(t *testing.T) {
	type args[T any] struct {
		ctx context.Context
		// db    sqlx.ExtContext
		table string
		where sq.Eq
	}
	tests := []struct {
		name        string
		args        args[TestStruct]
		expectFn    func(mock sqlmock.Sqlmock)
		want        []TestStruct
		wantErr     bool
		wantRowsErr bool
	}{
		{
			"ok",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				where: sq.Eq{"id": 1},
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(testStructSelect).
					WithArgs(1).
					WillReturnRows(sqlmock.NewRows(testStructCols).AddRow(testStructBinds...)).
					RowsWillBeClosed()
			},
			[]TestStruct{filledStruct},
			false,
			false,
		},
		{
			"2 rows",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				where: sq.Eq{"id": 1},
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(testStructSelect).
					WithArgs(1).
					WillReturnRows(sqlmock.NewRows(testStructCols).AddRow(testStructBinds...).AddRow(testStructBinds...)).
					RowsWillBeClosed()
			},
			[]TestStruct{filledStruct, filledStruct},
			false,
			false,
		},
		{
			"query error on the second row",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				where: sq.Eq{"id": 1},
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(testStructSelect).
					WithArgs(1).
					WillReturnRows(sqlmock.NewRows(testStructCols).
						AddRow(testStructBinds...).
						AddRow(testStructBinds...).RowError(1, assert.AnError),
					).RowsWillBeClosed()
			},
			[]TestStruct{filledStruct},
			false,
			true,
		},
		{
			"query error",
			args[TestStruct]{
				ctx:   context.Background(),
				table: "test_table",
				where: sq.Eq{"id": 1},
			},
			func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(testStructSelect).
					WithArgs(1).
					WillReturnError(assert.AnError)
			},
			nil,
			true,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock := sqlhelptest.InitMockDB(t)
			tt.expectFn(mock)
			got, err := Select[TestStruct](tt.args.ctx, db, tt.args.table, tt.args.where)
			if (err != nil) != tt.wantErr {
				t.Errorf("Select() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil && len(tt.want) == 0 {
				// no need to check further
				return
			}
			// collecting results
			var (
				results = make([]TestStruct, 0, len(tt.want))
				rowErrs error
			)
			for r, err := range got {
				if err != nil {
					rowErrs = errors.Join(err)
					continue
				}
				results = append(results, r)
			}
			if (rowErrs != nil) != tt.wantRowsErr {
				t.Errorf("Select() row error = %v, wantErr %v", rowErrs, tt.wantRowsErr)
			}
			assert.Equal(t, tt.want, results)
		})
	}
}

func TestExists(t *testing.T) {
	var (
		setupFn = func(t *testing.T, ctx context.Context, db sqlx.ExtContext) {
			t.Helper()
			var stmt = []string{
				"CREATE TABLE test_table (id INTEGER PRIMARY KEY)",
				"INSERT INTO test_table (id) VALUES (1)",
			}
			for _, s := range stmt {
				if _, err := db.ExecContext(ctx, s); err != nil {
					t.Fatal(err)
				}
			}
		}
		teardownFn = func(t *testing.T, ctx context.Context, db sqlx.ExtContext) {
			t.Helper()
			if _, err := db.ExecContext(ctx, "DROP TABLE test_table"); err != nil {
				t.Fatal(err)
			}
		}
	)

	type args struct {
		ctx context.Context
		// db    sqlx.ExtContext
		table string
		where sq.Sqlizer
	}
	tests := []struct {
		name       string
		args       args
		setupFn    func(t *testing.T, ctx context.Context, db sqlx.ExtContext)
		tearDownFn func(t *testing.T, ctx context.Context, db sqlx.ExtContext)
		want       bool
		wantErr    bool
	}{
		{
			"exists",
			args{
				ctx:   context.Background(),
				table: "test_table",
				where: sq.Eq{"id": 1},
			},
			setupFn,
			teardownFn,
			true,
			false,
		},
		{
			"not exists",
			args{
				ctx:   context.Background(),
				table: "test_table",
				where: sq.Eq{"id": 2},
			},
			setupFn,
			teardownFn,
			false,
			false,
		},
		{
			"database error",
			args{
				ctx:   context.Background(),
				table: "wrong_table",
				where: sq.Eq{"id": 1},
			},
			setupFn,
			teardownFn,
			false,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := sqlhelptest.InitSqliteDB(t)
			if tt.setupFn != nil {
				tt.setupFn(t, tt.args.ctx, db)
			}
			t.Cleanup(func() {
				if tt.tearDownFn != nil {
					tt.tearDownFn(t, tt.args.ctx, db)
				}
			})
			got, err := Exists(tt.args.ctx, db, tt.args.table, tt.args.where)
			if (err != nil) != tt.wantErr {
				t.Errorf("Exists() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Exists() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollect2(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		db, mock := sqlhelptest.InitMockDB(t)
		mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(testStructCols).AddRow(testStructBinds...))
		iter, err := Select[TestStruct](context.Background(), db, "test_table", sq.Eq{"id": 1})
		if err != nil {
			t.Fatal(err)
		}
		got, err := Collect2(iter)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, []TestStruct{filledStruct}, got)
	})
	t.Run("error", func(t *testing.T) {
		// make sure that the error is propagated
		db, mock := sqlhelptest.InitMockDB(t)
		mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(testStructCols).AddRow(testStructBinds...).RowError(0, assert.AnError))
		iter, err := Select[TestStruct](context.Background(), db, "test_table", sq.Eq{"id": 1})
		if err != nil {
			t.Fatal(err)
		}
		_, err = Collect2(iter)
		assert.ErrorIs(t, err, assert.AnError)
	})
}

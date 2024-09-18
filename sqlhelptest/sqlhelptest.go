// package sqlhelptest contains helper functions for testing database
// functions.
package sqlhelptest

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

// Driver that will be emulated for the mock db.
var Driver = "postgres"

// ExpectFunc is a function that sets up the expectations for the mock.
type ExpectFunc func(mock sqlmock.Sqlmock)

// InitMockDB returns a [sqlx.DB] and [sqlmock.Sqlmock] for testing.  On
// cleanup, it will check if all expectations were met.  Your t.Run may look
// like this:
//
//	t.Run("Test", func(t *testing.T) {
//	  db, mock := InitMockDB(t)
//	  expectFn(mock)
//	  // test your function
//	})
//
// With the expectFn being a function that sets up the expectations for the
// mock (of type [ExpectFunc]), add it to your tests struct like so:
//
//	tests := []struct {
//	  name     string
//	  fields   fields
//	  args     args
//	  expectFn sqlhelptest.ExpectFunc
//	  want     int64
//	  wantErr  bool
//	} //...
func InitMockDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	dbx := sqlx.NewDb(db, Driver)

	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	})

	return dbx, mock
}

func InitSqliteDB(t *testing.T) *sqlx.DB {
	t.Helper()

	db, err := sqlx.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
	})
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}

	return db
}

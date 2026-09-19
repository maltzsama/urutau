package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"

	errs "github.com/maltzsama/urutau/internal/errors"
)

func init() {
	errs.RegisterClassifier(classifyPgError)
}

// classifyPgError maps a PostgreSQL driver error to a failure category via its
// SQLSTATE code. It claims only *pgconn.PgError; every other error falls
// through to the next classifier.
func classifyPgError(err error) (errs.Failure, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return errs.Unknown, false
	}
	return errs.ClassifySQLState(pgErr.Code), true
}

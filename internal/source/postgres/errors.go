package postgres

// retryableSQLState reports whether a PostgreSQL SQLSTATE is transient, per
// the error-code appendix:
//
//   - class 08 — connection exception
//   - class 53 — insufficient resources (53300 too many connections, …)
//   - class 57 — operator intervention (57P01 admin shutdown, 57P03 cannot connect now)
//   - 40001 serialization failure, 40P01 deadlock detected
//
// Everything else is permanent: a bad password, a missing table, a syntax
// error — retrying only delays the inevitable.
func retryableSQLState(code string) bool {
	if len(code) >= 2 {
		switch code[:2] {
		case "08", "53", "57":
			return true
		}
	}
	switch code {
	case "40001", "40P01":
		return true
	}
	return false
}

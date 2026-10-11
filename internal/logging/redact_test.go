package logging

import "testing"

func TestRedactURI(t *testing.T) {
	for _, tc := range []struct {
		name, in, want string
	}{
		{"query password", "clickhouse://clickhouse.e2e.svc:9000?password=clickpass", "clickhouse://clickhouse.e2e.svc:9000?password=REDACTED"},
		{"userinfo password", "postgres://app:s3cret@db:5432/shop?sslmode=disable", "postgres://app:REDACTED@db:5432/shop?sslmode=disable"},
		{"both", "clickhouse://u:p@h:9000/db?secure=true&password=x", "clickhouse://u:REDACTED@h:9000/db?password=REDACTED&secure=true"},
		{"token and secret keys", "https://h/api?access_token=abc&client_secret=def&x=1", "https://h/api?access_token=REDACTED&client_secret=REDACTED&x=1"},
		{"nothing to hide", "http://polaris.e2e.svc.cluster.local:8181/api/catalog", "http://polaris.e2e.svc.cluster.local:8181/api/catalog"},
		{"user without password", "couchbase://app@couchbase.e2e.svc", "couchbase://app@couchbase.e2e.svc"},
		{"bare address", "clickhouse.e2e.svc:9000", "clickhouse.e2e.svc:9000"},
		{"empty", "", ""},
		{"unparseable with credentials", "mysql://app:pa ss@tcp(db:3306)/shop", "REDACTED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := RedactURI(tc.in); got != tc.want {
				t.Errorf("RedactURI(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

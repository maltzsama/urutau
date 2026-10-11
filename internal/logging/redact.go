package logging

import (
	"net/url"
	"strings"
)

// redacted replaces a credential in a logged URI.
const redacted = "REDACTED"

// RedactURI returns uri fit for a log line: the password of its userinfo and
// the value of every query parameter that names a credential are replaced.
// A connection string is the one place a sink or source keeps its secret
// inline (clickhouse://host?password=..., postgres://user:pass@host), and a
// startup line that prints it verbatim leaks the secret to every log reader.
//
// A string that does not parse as a URI is returned as is when it carries no
// "@" or "=" (a bare host:port), and replaced whole otherwise: guessing where
// the secret sits in an unknown format is how one leaks.
func RedactURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme == "" {
		if strings.ContainsAny(uri, "@=") {
			return redacted
		}
		return uri
	}
	if _, ok := u.User.Password(); ok {
		u.User = url.UserPassword(u.User.Username(), redacted)
	}
	q := u.Query()
	changed := false
	for k := range q {
		if credentialParam(k) {
			q.Set(k, redacted)
			changed = true
		}
	}
	if changed {
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// credentialParam reports whether a query parameter name denotes a secret.
func credentialParam(name string) bool {
	n := strings.ToLower(name)
	for _, s := range []string{"password", "passwd", "pwd", "secret", "token", "apikey", "api_key", "access_key", "credential"} {
		if strings.Contains(n, s) {
			return true
		}
	}
	return false
}

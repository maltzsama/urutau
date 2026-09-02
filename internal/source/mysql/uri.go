package mysql

import (
	"fmt"
	"net/url"
	"strings"
)

// URI is a parsed MySQL source URI.
type URI struct {
	User, Password, Host, Port, DB string
}

// ParseURI parses a "mysql://user:pass@host:port/db" source URI.
func ParseURI(uri string) (*URI, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("mysql: source uri: %w", err)
	}
	if u.Scheme != "mysql" {
		return nil, fmt.Errorf("mysql: source uri scheme %q, want mysql", u.Scheme)
	}
	c := &URI{User: u.User.Username()}
	if p, ok := u.User.Password(); ok {
		c.Password = p
	}
	c.Host = u.Hostname()
	if c.Host == "" {
		return nil, fmt.Errorf("mysql: source uri %q lacks host", uri)
	}
	if p := u.Port(); p != "" {
		c.Port = p
	} else {
		c.Port = "3306"
	}
	c.DB = strings.TrimPrefix(u.Path, "/")
	if c.DB == "" {
		return nil, fmt.Errorf("mysql: source uri %q lacks /db", uri)
	}
	return c, nil
}

// Addr returns host:port.
func (c *URI) Addr() string { return c.Host + ":" + c.Port }

// QueryDSN renders the go-sql-driver DSN for the query connection.
func (c *URI) QueryDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", c.User, c.Password, c.Addr(), c.DB)
}

package catalog

import (
	"strings"
)

// QuoteIdent quotes one identifier, doubling embedded quotes.
func QuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// QuoteIdents quotes each identifier.
func QuoteIdents(cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = QuoteIdent(strings.TrimSpace(c))
	}
	return out
}

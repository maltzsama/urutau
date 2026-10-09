package spec

import "fmt"

// validateOnTruncate checks the destructive-statement policy. It applies to
// the relational sources that can carry TRUNCATE/destructive DDL (postgres,
// mysql); other kinds, and an empty value, resolve to the default "ignore".
func validateOnTruncate(kind, onTruncate string, problems *[]string) {
	if onTruncate == "" {
		return
	}
	if kind != "postgres" && kind != "mysql" {
		*problems = append(*problems, fmt.Sprintf("source.onTruncate: only valid for kind postgres or mysql, not %q", kind))
		return
	}
	switch onTruncate {
	case "ignore", "fail":
	default:
		*problems = append(*problems, fmt.Sprintf("source.onTruncate: unsupported %q (want ignore | fail; propagate is not implemented)", onTruncate))
	}
}

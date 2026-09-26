package architecture

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The size ratchet (issue #398): files and functions may not grow past fixed
// limits, and today's offenders may not grow past their current size. The
// allowlist only shrinks: an entry for an item back under the limit, or gone,
// is stale and fails too, so the file stays honest.
const (
	maxFileLines = 800
	maxFuncLines = 120
	allowlist    = "size_allowlist.txt"
)

// sizeRoots are the production trees measured, relative to the module root.
// test/, examples/ and hack/ hold test fixtures and tooling.
var sizeRoots = []string{"api", "cmd", "core", "dataplane", "driver", "internal", "position", "sink", "source", "spec"}

// sizeItem is one measured file ("path") or function ("path:Recv.Name").
type sizeItem struct {
	key   string
	lines int
	limit int
}

// measureSizes returns every production file and function of the module with
// its line count, keyed as the allowlist keys them.
func measureSizes(t *testing.T, root string) []sizeItem {
	t.Helper()
	var items []sizeItem
	fset := token.NewFileSet()
	for _, dir := range sizeRoots {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			name := d.Name()
			if d.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") ||
				strings.HasSuffix(name, ".pb.go") || strings.HasPrefix(name, "zz_generated") {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			items = append(items, sizeItem{key: rel, lines: fset.File(f.Pos()).LineCount(), limit: maxFileLines})
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				// From `func` to the closing brace; the doc comment is not code.
				n := fset.Position(fn.End()).Line - fset.Position(fn.Type.Func).Line + 1
				items = append(items, sizeItem{key: rel + ":" + funcName(fn), lines: n, limit: maxFuncLines})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("measure %s: %v", dir, err)
		}
	}
	return items
}

// funcName is "Name" or "Recv.Name" (the receiver's type, pointer dropped).
func funcName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	typ := fn.Recv.List[0].Type
	if star, ok := typ.(*ast.StarExpr); ok {
		typ = star.X
	}
	switch g := typ.(type) { // generic receiver T[P] or T[P, Q]
	case *ast.IndexExpr:
		typ = g.X
	case *ast.IndexListExpr:
		typ = g.X
	}
	if id, ok := typ.(*ast.Ident); ok {
		return id.Name + "." + fn.Name.Name
	}
	return fn.Name.Name
}

// allowEntry is one allowlist line: the item, its recorded maximum, and the
// justification comment carried with it.
type allowEntry struct {
	max     int
	comment string
}

func readAllowlist(t *testing.T, path string) map[string]allowEntry {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	out := map[string]allowEntry{}
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		body, comment, _ := strings.Cut(line, "#")
		fields := strings.Fields(body)
		if len(fields) != 2 {
			t.Fatalf("%s:%d: want `path[:Func] <max-lines> [# why]`, got %q", allowlist, n, line)
		}
		max, err := strconv.Atoi(fields[1])
		if err != nil {
			t.Fatalf("%s:%d: max lines %q: %v", allowlist, n, fields[1], err)
		}
		if _, dup := out[fields[0]]; dup {
			t.Fatalf("%s:%d: duplicate entry %s", allowlist, n, fields[0])
		}
		out[fields[0]] = allowEntry{max: max, comment: strings.TrimSpace(comment)}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return out
}

// sizeProblems checks the measured items against the limits and the
// allowlist.
func sizeProblems(items []sizeItem, allow map[string]allowEntry) []string {
	var out []string
	seen := map[string]bool{}
	for _, it := range items {
		seen[it.key] = true
		e, listed := allow[it.key]
		switch {
		case listed && e.comment == "":
			out = append(out, fmt.Sprintf("%s: allowlist entry has no `# why` comment — an exemption must say why", it.key))
		case listed && it.lines <= it.limit:
			out = append(out, fmt.Sprintf("stale entry %s: it is %d lines, within the %d-line limit — remove it from %s", it.key, it.lines, it.limit, allowlist))
		case listed && it.lines > e.max:
			out = append(out, fmt.Sprintf("%s grew to %d lines, past its allowlisted %d (limit %d) — split it; the allowlist only shrinks", it.key, it.lines, e.max, it.limit))
		case !listed && it.lines > it.limit:
			out = append(out, fmt.Sprintf("%s is %d lines, over the %d-line limit — split it, or for a justified exemption add `%s %d # why` to %s in the same PR", it.key, it.lines, it.limit, it.key, it.lines, allowlist))
		}
	}
	for key := range allow {
		if !seen[key] {
			out = append(out, fmt.Sprintf("stale entry %s: no such file or function — remove it from %s", key, allowlist))
		}
	}
	sort.Strings(out)
	return out
}

// TestSizeRatchet enforces the limits. URUTAU_SIZE_ALLOWLIST_WRITE=1 rewrites
// the allowlist to the current offenders at their current sizes, keeping each
// entry's comment — for shrinking entries after a split, never for growing
// one (review the diff).
func TestSizeRatchet(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	items := measureSizes(t, root)
	allow := readAllowlist(t, allowlist)
	if os.Getenv("URUTAU_SIZE_ALLOWLIST_WRITE") == "1" {
		writeAllowlist(t, items, allow)
		return
	}
	for _, p := range sizeProblems(items, allow) {
		t.Error(p)
	}
}

func writeAllowlist(t *testing.T, items []sizeItem, old map[string]allowEntry) {
	t.Helper()
	var b strings.Builder
	b.WriteString(allowlistHeader)
	var lines []string
	for _, it := range items {
		if it.lines <= it.limit {
			continue
		}
		line := fmt.Sprintf("%s %d", it.key, it.lines)
		if c := old[it.key].comment; c != "" {
			line += " # " + c
		}
		lines = append(lines, line)
	}
	sort.Strings(lines)
	b.WriteString(strings.Join(lines, "\n") + "\n")
	if err := os.WriteFile(allowlist, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

const allowlistHeader = `# Size ratchet allowlist (issue #398; see TestSizeRatchet).
#
# Limits for production code: 800 lines per file, 120 per function (func to
# closing brace). Each entry holds one item at most at its recorded size:
#
#   path[:Recv.Func] <max-lines> [# why it is exempt]
#
# The list only shrinks. An entry fails when its item grows past it, and when
# the item drops back under the limit or no longer exists (a stale entry).
# Every entry says why. The ones the ratchet started from read
# "baseline (#398)": code to split under the #397 restructuring.
`

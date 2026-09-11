package enrich

// P8 of BRIEF-PERF v10 Onda 2 — the prohibitions, enforced by AST walk.
//
// These tests parse the NON-TEST .go files of internal/enrich (and, for
// TestNoPanicOutsideTest, internal/dataplane too) with go/ast — no type
// info, no build, no shell. They are name-heuristic by design: a scalar
// slice is recognized by its element type name against a fixed list, so
// renaming a mask to "banana" is still caught as []bool.
//
//	P1  no `for range` over a slice-of-scalar, no `for` whose condition
//	    calls .NumRows()/.Len() — unless the line above carries
//	    //allow:rowloop <reason>
//	P4  no map[string]any / map[string]map[string]any
//	P7  no panic() in non-test files
//
// The one exception marker is //allow:rowloop. The expected markers in the
// system, all listed here:
//
//	internal/enrich/loader_sql.go  — Row 1 scanTargets alloc, Row 1 the
//	                                 scan loop, Row 2 the appendTyped switch
//	internal/enrich/enrich.go      — snapshot key index (once per refresh)
//	internal/enrich/columnar.go    — the ref-column gather pass
//	internal/dataplane/collapse.go — composite-PK hashing, LWW grouping,
//	                                 distinct-key winner list

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var scalarElemNames = map[string]bool{
	"bool": true, "byte": true, "rune": true, "string": true, "any": true,
	"int": true, "int8": true, "int16": true, "int32": true, "int64": true,
	"uint": true, "uint8": true, "uint16": true, "uint32": true, "uint64": true,
	"float32": true, "float64": true,
	// project scalar aliases whose []T is still row data:
	"Op": true,
}

func nonTestGoFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var out []string
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		out = append(out, filepath.Join(dir, n))
	}
	return out
}

// allowMarkedLines returns the set of 1-indexed line numbers that carry a
// //allow:rowloop comment (the marker applies to the NEXT statement, so a
// loop on line N is allowed if N-1..N carry the marker).
func allowMarkedLines(src []byte) map[int]bool {
	marked := map[int]bool{}
	for i, line := range strings.Split(string(src), "\n") {
		if strings.Contains(line, "//allow:rowloop") {
			marked[i+1] = true // the marker line
			marked[i+2] = true // the statement it guards
			marked[i+3] = true // ... allowing one blank/comment line between
		}
	}
	return marked
}

// sliceElemName returns the element type name of an *ast.ArrayType with no
// length (a slice), or "" if X is not a bare slice type expression.
func sliceElemName(e ast.Expr) string {
	at, ok := e.(*ast.ArrayType)
	if !ok || at.Len != nil {
		return ""
	}
	switch elt := at.Elt.(type) {
	case *ast.Ident:
		return elt.Name
	case *ast.SelectorExpr:
		return elt.Sel.Name
	case *ast.InterfaceType:
		if elt.Methods == nil || len(elt.Methods.List) == 0 {
			return "any"
		}
	}
	return ""
}

func TestNoRowLoopsInEnrich(t *testing.T) {
	fset := token.NewFileSet()
	for _, path := range nonTestGoFiles(t, ".") {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		marked := allowMarkedLines(src)

		// Track local vars: (a) declared as / made a slice-of-scalar, so a
		// `for range x` where x was `make([]bool, n)` is caught; (b) bound
		// to a .Len()/.NumRows() result, so `n := a.Len(); for i:=0;i<n;i++`
		// is caught even though the loop condition names only `n`.
		scalarVars := map[string]bool{}
		lenVars := map[string]bool{}
		ast.Inspect(f, func(n ast.Node) bool {
			switch d := n.(type) {
			case *ast.ValueSpec:
				if name := sliceElemName(d.Type); scalarElemNames[name] {
					for _, id := range d.Names {
						scalarVars[id.Name] = true
					}
				}
			case *ast.AssignStmt:
				for i, rhs := range d.Rhs {
					call, ok := rhs.(*ast.CallExpr)
					if !ok {
						continue
					}
					if i >= len(d.Lhs) {
						continue
					}
					lid, lok := d.Lhs[i].(*ast.Ident)
					if !lok {
						continue
					}
					if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "make" && len(call.Args) > 0 {
						if name := sliceElemName(call.Args[0]); scalarElemNames[name] {
							scalarVars[lid.Name] = true
						}
					}
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						if sel.Sel.Name == "Len" || sel.Sel.Name == "NumRows" {
							lenVars[lid.Name] = true
						}
					}
				}
			}
			return true
		})

		ast.Inspect(f, func(n ast.Node) bool {
			pos := fset.Position(nodePos(n))
			switch stmt := n.(type) {
			case *ast.RangeStmt:
				if marked[pos.Line] {
					return true
				}
				// range over a composite literal []T{...}
				if name := sliceElemName(stmt.X); scalarElemNames[name] {
					t.Errorf("%s:%d: P1 — range over slice-of-scalar []%s without //allow:rowloop", path, pos.Line, name)
				}
				// range over a known scalar var
				if id, ok := stmt.X.(*ast.Ident); ok && scalarVars[id.Name] {
					t.Errorf("%s:%d: P1 — range over slice-of-scalar var %q without //allow:rowloop", path, pos.Line, id.Name)
				}
				// range over a []T conversion or slice-of-scalar arg is
				// harder without types; the make/decl tracking above covers
				// the realistic cases.
			case *ast.ForStmt:
				if marked[pos.Line] {
					return true
				}
				if stmt.Cond != nil && (condHasDataLen(stmt.Cond) || condRefsVar(stmt.Cond, lenVars)) {
					t.Errorf("%s:%d: P1 — for loop bounded by .NumRows()/.Len() without //allow:rowloop", path, pos.Line)
				}
			}
			return true
		})
	}
}

func nodePos(n ast.Node) token.Pos {
	if n == nil {
		return token.NoPos
	}
	return n.Pos()
}

// condHasDataLen reports whether the expression contains a call to a method
// named NumRows or Len.
func condHasDataLen(e ast.Expr) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if sel.Sel.Name == "NumRows" || sel.Sel.Name == "Len" {
			found = true
		}
		return true
	})
	return found
}

// condRefsVar reports whether the expression names any identifier in vars.
func condRefsVar(e ast.Expr, vars map[string]bool) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && vars[id.Name] {
			found = true
		}
		return true
	})
	return found
}

func TestForbiddenMaps(t *testing.T) {
	fset := token.NewFileSet()
	for _, path := range nonTestGoFiles(t, ".") {
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			mt, ok := n.(*ast.MapType)
			if !ok {
				return true
			}
			kid, kok := mt.Key.(*ast.Ident)
			if !kok || kid.Name != "string" {
				return true
			}
			// map[string]any
			if isEmptyIface(mt.Value) {
				t.Errorf("%s:%d: P4 — map[string]any", path, fset.Position(mt.Pos()).Line)
			}
			// map[string]map[string]any
			if inner, iok := mt.Value.(*ast.MapType); iok {
				if iid, ok := inner.Key.(*ast.Ident); ok && iid.Name == "string" && isEmptyIface(inner.Value) {
					t.Errorf("%s:%d: P4 — map[string]map[string]any", path, fset.Position(mt.Pos()).Line)
				}
			}
			return true
		})
	}
}

func isEmptyIface(e ast.Expr) bool {
	if id, ok := e.(*ast.Ident); ok {
		return id.Name == "any"
	}
	it, ok := e.(*ast.InterfaceType)
	return ok && (it.Methods == nil || len(it.Methods.List) == 0)
}

func TestNoPanicOutsideTest(t *testing.T) {
	fset := token.NewFileSet()
	dirs := []string{".", "../dataplane"}
	for _, dir := range dirs {
		for _, path := range nonTestGoFiles(t, dir) {
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "panic" {
					t.Errorf("%s:%d: P7 — panic() in non-test code", path, fset.Position(call.Pos()).Line)
				}
				return true
			})
		}
	}
}

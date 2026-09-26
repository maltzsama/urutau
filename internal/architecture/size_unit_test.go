package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func TestSizeProblems(t *testing.T) {
	items := []sizeItem{
		{key: "a.go", lines: 900, limit: maxFileLines},                    // listed at 900: ok
		{key: "a.go:Big", lines: 130, limit: maxFuncLines},                // listed at 125: grew
		{key: "a.go:Small", lines: 40, limit: maxFuncLines},               // listed: now under the limit
		{key: "b.go:New", lines: maxFuncLines + 1, limit: maxFuncLines},   // not listed, over
		{key: "b.go:Edge", lines: maxFuncLines, limit: maxFuncLines},      // exactly the limit: ok
		{key: "c.go", lines: maxFileLines + 1, limit: maxFileLines},       // not listed, over
		{key: "d.go:Split", lines: maxFuncLines + 5, limit: maxFuncLines}, // listed higher: shrank, still over: ok
	}
	allow := map[string]allowEntry{
		"a.go": {max: 900}, "a.go:Big": {max: 125}, "a.go:Small": {max: 200},
		"d.go:Split": {max: 300}, "gone.go:F": {max: 150},
	}
	got := strings.Join(sizeProblems(items, allow), "\n")
	for _, want := range []string{
		"a.go:Big grew to 130 lines, past its allowlisted 125 (limit 120)",
		"stale entry a.go:Small: it is 40 lines, within the 120-line limit",
		"b.go:New is 121 lines, over the 120-line limit — split it, or for a justified exemption add `b.go:New 121 # why`",
		"c.go is 801 lines, over the 800-line limit",
		"stale entry gone.go:F: no such file or function",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("problems lack %q:\n%s", want, got)
		}
	}
	if n := len(sizeProblems(items, allow)); n != 5 {
		t.Errorf("%d problems, want 5:\n%s", n, got)
	}
}

func TestFuncName(t *testing.T) {
	src := `package p
func F() {}
func (s *S) M() {}
func (v V) N() {}
func (g *G[T]) O() {}
`
	f, err := parser.ParseFile(token.NewFileSet(), "p.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, d := range f.Decls {
		got = append(got, funcName(d.(*ast.FuncDecl)))
	}
	if strings.Join(got, ",") != "F,S.M,V.N,G.O" {
		t.Fatalf("names = %v", got)
	}
}

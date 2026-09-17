package clickhouse

import (
	"testing"

	"github.com/maltzsama/urutau/core"
)

func TestPosQuoted(t *testing.T) {
	ti := tableIdent{db: "lakehouse", table: "orders"}
	got := ti.posQuoted()
	want := "`" + "lakehouse" + "`." + "`" + "orders_urutau_position" + "`"
	if got != want {
		t.Errorf("posQuoted() = %q, want %q", got, want)
	}
}

func TestQuoteIdentEscapes(t *testing.T) {
	got := quoteIdent("mytable")
	want := "`" + "mytable" + "`"
	if got != want {
		t.Errorf("quoteIdent = %q, want %q", got, want)
	}
	// Backtick inside is escaped: strings.ReplaceAll replaces ` with \x60
	got = quoteIdent("my\x60table")
	if len(got) < 4 || got[0] != '`' || got[len(got)-1] != '`' {
		t.Fatalf("quoteIdent with backtick not wrapped: %q", got)
	}
	inner := got[1 : len(got)-1]
	// ReplaceAll replaces ` with \x60, so backtick becomes backslash+backtick
	wantInner := "my\x5c\x60table"
	if inner != wantInner {
		t.Errorf("quoteIdent with backtick inner = %q, want %q", inner, wantInner)
	}
}

func TestSinkIdentWithDot(t *testing.T) {
	s := &Sink{ns: "default"}
	got, err := s.ident("mydb.mytable")
	if err != nil {
		t.Fatalf("ident: %v", err)
	}
	if got.db != "mydb" || got.table != "mytable" {
		t.Errorf("ident = %+v, want db=mydb table=mytable", got)
	}
}

func TestSinkIdentBareName(t *testing.T) {
	s := &Sink{ns: "default"}
	got, err := s.ident("orders")
	if err != nil {
		t.Fatalf("ident: %v", err)
	}
	if got.db != "default" || got.table != "orders" {
		t.Errorf("ident = %+v, want db=default table=orders", got)
	}
}

func TestSinkIdentNoNamespace(t *testing.T) {
	s := &Sink{}
	_, err := s.ident("orders")
	if err == nil {
		t.Error("unqualified name without namespace: want error")
	}
}

func TestTargetKey(t *testing.T) {
	s := &Sink{}
	got, err := s.targetKey(core.TableRef{Target: "orders"})
	if err != nil {
		t.Fatalf("targetKey: %v", err)
	}
	if got != "orders" {
		t.Errorf("targetKey = %q, want orders", got)
	}
	_, err = s.targetKey(core.TableRef{})
	if err == nil {
		t.Error("empty target: want error")
	}
}

func TestProgressIdent(t *testing.T) {
	s := &Sink{ns: "lakehouse"}
	got := s.progressIdent()
	want := "`" + "lakehouse" + "`." + "`" + "urutau_progress" + "`"
	if got != want {
		t.Errorf("progressIdent() = %q, want %q", got, want)
	}
}

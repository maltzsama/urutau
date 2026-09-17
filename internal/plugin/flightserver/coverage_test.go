package flightserver

// Coverage for the in-process Flight server: the pure projection helpers
// (toInt32/toInt64/appendScalar/coreSchemaToArrowFields/tableByName), the
// bearer auth path (hasBearer + the unary/stream interceptors), the socket
// path helper, and the Start/Addr/Stop lifecycle.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

func TestToInt32AndToInt64(t *testing.T) {
	if toInt32(int32(1)) != 1 || toInt32(int64(2)) != 2 || toInt32(3) != 3 || toInt32("x") != 0 {
		t.Fatal("toInt32 mapping is wrong")
	}
	if toInt64(int64(1)) != 1 || toInt64(int32(2)) != 2 || toInt64(3) != 3 || toInt64("x") != 0 {
		t.Fatal("toInt64 mapping is wrong")
	}
}

func TestAppendScalarBranches(t *testing.T) {
	boolB := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer boolB.Release()
	appendScalar(boolB, true, true)
	if boolB.Len() != 1 || !boolB.NewBooleanArray().Value(0) {
		t.Fatal("bool append failed")
	}

	int32B := array.NewInt32Builder(memory.DefaultAllocator)
	defer int32B.Release()
	appendScalar(int32B, int64(7), true)
	if int32B.NewInt32Array().Value(0) != 7 {
		t.Fatal("int32 append failed")
	}

	int64B := array.NewInt64Builder(memory.DefaultAllocator)
	defer int64B.Release()
	appendScalar(int64B, int32(8), true)
	if int64B.NewInt64Array().Value(0) != 8 {
		t.Fatal("int64 append failed")
	}

	uintB := array.NewUint64Builder(memory.DefaultAllocator)
	defer uintB.Release()
	appendScalar(uintB, uint64(9), true)
	if uintB.NewUint64Array().Value(0) != 9 {
		t.Fatal("uint64 append failed")
	}

	f32B := array.NewFloat32Builder(memory.DefaultAllocator)
	defer f32B.Release()
	appendScalar(f32B, float32(1.5), true)
	if f32B.NewFloat32Array().Value(0) != 1.5 {
		t.Fatal("float32 append failed")
	}

	f64B := array.NewFloat64Builder(memory.DefaultAllocator)
	defer f64B.Release()
	appendScalar(f64B, float64(2.5), true)
	if f64B.NewFloat64Array().Value(0) != 2.5 {
		t.Fatal("float64 append failed")
	}

	strB := array.NewStringBuilder(memory.DefaultAllocator)
	defer strB.Release()
	appendScalar(strB, 42, true)
	if strB.NewStringArray().Value(0) != "42" {
		t.Fatal("string append failed")
	}

	binB := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer binB.Release()
	appendScalar(binB, []byte{0x1}, true)
	appendScalar(binB, "raw", true)
	arr := binB.NewBinaryArray()
	if string(arr.Value(0)) != "\x01" || string(arr.Value(1)) != "raw" {
		t.Fatal("binary append failed")
	}

	tsB := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Microsecond})
	defer tsB.Release()
	appendScalar(tsB, time.Unix(1, 0), true)
	appendScalar(tsB, "not-a-time", true)
	tsArr := tsB.NewTimestampArray()
	if tsArr.IsNull(0) || !tsArr.IsNull(1) {
		t.Fatal("timestamp append must accept time.Time and null non-times")
	}

	// An unhandled builder and a null/absent value both append null.
	int8B := array.NewInt8Builder(memory.DefaultAllocator)
	defer int8B.Release()
	appendScalar(int8B, "x", true)
	appendScalar(int8B, nil, true)
	appendScalar(int8B, "x", false)
	if int8B.Len() != 3 || !int8B.NewInt8Array().IsNull(0) {
		t.Fatal("default/null appends must be null")
	}
}

func TestCoreSchemaToArrowFields(t *testing.T) {
	cs := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}}
	fields, err := coreSchemaToArrowFields(cs)
	if err != nil {
		t.Fatalf("coreSchemaToArrowFields: %v", err)
	}
	if len(fields) != 2 || fields[0].Name != "id" || fields[1].Name != "v" {
		t.Fatalf("fields = %v, want the two data columns only", fields)
	}
}

func TestTableByName(t *testing.T) {
	s := &SourceServer{spec: &spec.Spec{Tables: []spec.Table{{Source: "shop.orders"}}}}
	if _, ok := s.tableByName("shop.orders"); !ok {
		t.Fatal("known table must resolve")
	}
	if _, ok := s.tableByName("nope"); ok {
		t.Fatal("unknown table must not resolve")
	}
	if _, ok := (&SourceServer{}).tableByName("x"); ok {
		t.Fatal("nil spec must not resolve")
	}
}

// ── auth ─────────────────────────────────────────────────────────────

func TestHasBearer(t *testing.T) {
	with := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret"))
	if !hasBearer(with, "secret") {
		t.Fatal("a matching bearer must authenticate")
	}
	if hasBearer(with, "other") {
		t.Fatal("a wrong token must not authenticate")
	}
	if hasBearer(context.Background(), "secret") {
		t.Fatal("no metadata must not authenticate")
	}
	short := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bear"))
	if hasBearer(short, "secret") {
		t.Fatal("a too-short header must not authenticate")
	}
	plain := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "secret"))
	if hasBearer(plain, "secret") {
		t.Fatal("a missing Bearer prefix must not authenticate")
	}
}

type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeStream) Context() context.Context { return f.ctx }

func TestAuthInterceptorUnary(t *testing.T) {
	a := &authInterceptor{token: "secret"}
	called := false
	handler := func(context.Context, any) (any, error) { called = true; return "ok", nil }

	// Handshake is exempt.
	if _, err := a.unary(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/arrow.flight.protocol.FlightService/Handshake"}, handler); err != nil {
		t.Fatalf("handshake must pass: %v", err)
	}
	// A non-handshake method without a bearer is rejected.
	if _, err := a.unary(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x/Y"}, handler); err == nil {
		t.Fatal("a missing bearer must be rejected")
	}
	// With the right bearer it passes.
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret"))
	if _, err := a.unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/x/Y"}, handler); err != nil {
		t.Fatalf("authenticated unary must pass: %v", err)
	}
	if !called {
		t.Fatal("handler was not invoked")
	}
}

func TestAuthInterceptorStream(t *testing.T) {
	a := &authInterceptor{token: "secret"}
	handler := func(any, grpc.ServerStream) error { return nil }

	if err := a.stream(nil, &fakeStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/arrow.flight.protocol.FlightService/Handshake"}, handler); err != nil {
		t.Fatalf("handshake stream must pass: %v", err)
	}
	if err := a.stream(nil, &fakeStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "/x/Y"}, handler); err == nil {
		t.Fatal("a stream without a bearer must be rejected")
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret"))
	if err := a.stream(nil, &fakeStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: "/x/Y"}, handler); err != nil {
		t.Fatalf("authenticated stream must pass: %v", err)
	}
}

// ── lifecycle ────────────────────────────────────────────────────────

func TestSocketPath(t *testing.T) {
	p, err := socketPath("")
	if err != nil {
		t.Fatalf("socketPath: %v", err)
	}
	if filepath.Dir(p) != os.TempDir() || !strings.HasPrefix(filepath.Base(p), "urutau-plugin-") {
		t.Fatalf("socketPath = %q", p)
	}
	q, err := socketPath(t.TempDir())
	if err != nil {
		t.Fatalf("socketPath(dir): %v", err)
	}
	if p == q {
		t.Fatal("two sockets must differ")
	}
}

func TestStartAddrStop(t *testing.T) {
	srv, err := Start(t.TempDir(), "token", NewSourceServer(nil, &spec.Spec{}, source.Runtime{}))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if srv.Addr() == "" {
		t.Fatal("Addr must be set")
	}
	srv.Stop()
}

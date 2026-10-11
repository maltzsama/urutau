package chunk

import (
	"context"
	"testing"
)

func TestNewChunkerBadSource(t *testing.T) {
	_, err := New(context.Background(), nil, "no_schema_dot", "id", 1000)
	if err == nil {
		t.Error("bare source name: want error")
	}
}

func TestNewChunkerBadChunkSize(t *testing.T) {
	_, err := New(context.Background(), nil, "public.orders", "id", 0)
	if err == nil {
		t.Error("zero chunk size: want error")
	}
	_, err = New(context.Background(), nil, "public.orders", "id", -1)
	if err == nil {
		t.Error("negative chunk size: want error")
	}
}

func TestNewChunkerValid(t *testing.T) {
	c, err := New(context.Background(), nil, "public.orders", "id", 1000)
	if err != nil {
		t.Fatalf("NewChunker: %v", err)
	}
	if c.pk[0] != "id" {
		t.Errorf("pk = %v", c.pk)
	}
}

func TestNewChunkerCompositeKey(t *testing.T) {
	c, err := New(context.Background(), nil, "public.orders", "a, b", 1000)
	if err != nil {
		t.Fatalf("NewChunker: %v", err)
	}
	if len(c.pk) != 2 || c.pk[0] != "a" || c.pk[1] != "b" {
		t.Errorf("pk = %v", c.pk)
	}
}

func TestNewChunkerKeyWithSpaces(t *testing.T) {
	c, err := New(context.Background(), nil, "public.orders", " a , b , c ", 500)
	if err != nil {
		t.Fatalf("NewChunker: %v", err)
	}
	if len(c.pk) != 3 {
		t.Errorf("pk = %v", c.pk)
	}
}

package remote

import (
	"encoding/json"
	"fmt"

	"github.com/maltzsama/urutau/core"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/spec"
)

// enrichSpecs converts the assignment's reference joins into the spec shape
// the enrich stage consumes.
func enrichSpecs(refs []*pb.EnrichRef) []spec.Enrich {
	cfgs := make([]spec.Enrich, 0, len(refs))
	for _, e := range refs {
		cfgs = append(cfgs, spec.Enrich{
			Table: e.Table,
			Source: spec.EnrichSource{
				URI:   e.SourceUri,
				Query: e.SourceQuery,
			},
			On:           e.On,
			Select:       e.Select,
			As:           e.As,
			JoinType:     e.JoinType,
			Refresh:      e.Refresh,
			OnColdStart:  e.OnColdStart,
			BufferLimits: spec.EnrichBufferLimits{MaxEvents: int(e.BufferMaxEvents), MaxWait: e.BufferMaxWait},
		})
	}
	return cfgs
}

// parsePosition returns the parser for the assignment's source kind.
// Getting this wrong is not a minor inconvenience: a Kafka pipeline whose
// committed positions were parsed as GTID sets would fail the worker boot
// the moment the first cdc.position existed. Delegates to position.Parse so
// the sink's per-partition read (WK-001 C7) shares one mapping.
func parsePosition(kind string) func(string) (position.Position, error) {
	return func(s string) (position.Position, error) { return position.Parse(kind, s) }
}

// committedStrings renders the committed map for the wire Hello.
func committedStrings(m map[string]position.Position) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v.String()
	}
	return out
}

// decodeCastPolicy rebuilds the cast policy the coordinator shipped as JSON.
func decodeCastPolicy(ta *pb.TableAssignment) (core.CastPolicy, error) {
	if len(ta.CastPolicy) == 0 {
		return core.CastPolicy{}, nil
	}
	var cast core.CastPolicy
	if err := json.Unmarshal(ta.CastPolicy, &cast); err != nil {
		return core.CastPolicy{}, fmt.Errorf("worker: cast %s: %w", ta.TargetTable, err)
	}
	return cast, nil
}

// decodeMetadata rebuilds the metadata columns the coordinator shipped as
// JSON.
func decodeMetadata(ta *pb.TableAssignment) ([]core.MetadataColumn, error) {
	if len(ta.Metadata) == 0 {
		return nil, nil
	}
	var meta []core.MetadataColumn
	if err := json.Unmarshal(ta.Metadata, &meta); err != nil {
		return nil, fmt.Errorf("worker: metadata %s: %w", ta.TargetTable, err)
	}
	return meta, nil
}

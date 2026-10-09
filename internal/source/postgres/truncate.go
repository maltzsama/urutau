package postgres

import (
	"fmt"
	"strings"

	pglogrepl "github.com/jackc/pglogrepl"

	"github.com/maltzsama/urutau/source"
)

// handleTruncate surfaces a TRUNCATE the stream carried but the engine does
// not propagate: the sink diverges from the source. It reports each affected
// table to the engine (metric + run event) and, under onTruncate: fail, ends
// the stream with an error naming the table(s) so the operator reconciles the
// sink by hand (#671).
func (r *Reader) handleTruncate(payload []byte) error {
	msg := &pglogrepl.TruncateMessage{}
	if err := msg.Decode(payload); err != nil {
		return fmt.Errorf("postgres: truncate: %w", err)
	}
	tables := make([]string, 0, len(msg.RelationIDs))
	for _, relID := range msg.RelationIDs {
		// A relation not yet announced in this session still belongs to the
		// publication, so name it by OID rather than dropping the truncate.
		if entry, ok := r.relByID[relID]; ok {
			tables = append(tables, entry.ref.Source)
		} else {
			tables = append(tables, fmt.Sprintf("relation#%d", relID))
		}
	}
	if len(tables) == 0 {
		return nil
	}
	detail := strings.Join(tables, ", ")
	if r.cfg.OnDestructiveDDL != nil {
		for _, tbl := range tables {
			r.cfg.OnDestructiveDDL(source.DestructiveDDL{
				Source: "postgres", Kind: "truncate", Table: tbl, Detail: detail,
			})
		}
	}
	if r.cfg.OnTruncate == "fail" {
		return fmt.Errorf("postgres: TRUNCATE on replicated table(s) %s (onTruncate: fail)", detail)
	}
	r.cfg.Logger.Warn("postgres: truncate received; not propagated to the sink", "tables", detail)
	return nil
}

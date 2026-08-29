package iceberg

import (
	"context"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Append writes the record as a new data file in a fresh snapshot, with the
// given snapshot props and the cdc.position table property.
func Append(ctx context.Context, tbl *table.Table, rec arrow.RecordBatch, props iceberg.Properties) error {
	txn := tbl.NewTransaction()
	if err := txn.AppendTable(ctx, array.NewTableFromRecords(rec.Schema(), []arrow.RecordBatch{rec}), -1, props); err != nil {
		return err
	}
	setPositionProperty(txn, props)
	_, err := txn.Commit(ctx)
	return err
}

// AppendAndDelete appends a data file and an equality-delete file in a single
// transaction. In iceberg-go v0.6.0 this stages TWO snapshots (append, then
// delete); the delete carries the higher sequence number and applies to the
// freshly appended file too. It is kept as the naive pattern the spike uses to
// document the gotcha — a correct upsert must delete-then-append instead.
func AppendAndDelete(ctx context.Context, tbl *table.Table, data arrow.RecordBatch, eqFieldIDs []int, deletes arrow.RecordBatch, props iceberg.Properties) error {
	txn := tbl.NewTransaction()
	if err := txn.AppendTable(ctx, array.NewTableFromRecords(data.Schema(), []arrow.RecordBatch{data}), -1, props); err != nil {
		return err
	}
	if err := addEqualityDeletes(ctx, txn, eqFieldIDs, deletes, props); err != nil {
		return err
	}
	setPositionProperty(txn, props)
	_, err := txn.Commit(ctx)
	return err
}

// DeleteOnly commits an equality-delete file with no accompanying data row.
func DeleteOnly(ctx context.Context, tbl *table.Table, eqFieldIDs []int, deletes arrow.RecordBatch, props iceberg.Properties) error {
	txn := tbl.NewTransaction()
	if err := addEqualityDeletes(ctx, txn, eqFieldIDs, deletes, props); err != nil {
		return err
	}
	setPositionProperty(txn, props)
	_, err := txn.Commit(ctx)
	return err
}

func addEqualityDeletes(ctx context.Context, txn *table.Transaction, eqFieldIDs []int, deletes arrow.RecordBatch, props iceberg.Properties) error {
	files, err := txn.WriteEqualityDeletes(ctx, eqFieldIDs, oneBatch(deletes))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}
	return txn.NewRowDelta(props).AddDeletes(files...).Commit(ctx)
}

// setPositionProperty keeps the table property cdc.position in lockstep with
// the data committed by this transaction.
func setPositionProperty(txn *table.Transaction, props iceberg.Properties) {
	if pos, ok := props["cdc.position"]; ok {
		_ = txn.SetProperties(iceberg.Properties{"cdc.position": pos})
	}
}

// oneBatch adapts a single record batch to the iter.Seq2 shape expected by
// the equality-delete writer.
func oneBatch(rec arrow.RecordBatch) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		yield(rec, nil)
	}
}

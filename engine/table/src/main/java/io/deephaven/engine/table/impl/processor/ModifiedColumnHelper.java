//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.processor;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ColumnSource;

import java.util.function.BiPredicate;

final class ModifiedColumnHelper {

    public static <T> RowSet comparePrevChunkToChunk(
            ColumnSource<T> columnSource,
            RowSet rowSet,
            int chunkSize,
            BiPredicate<T, T> includePredicate) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        try (
                final FillContext context = columnSource.makeFillContext(chunkSize);
                final WritableLongChunk<OrderedRowKeys> keys = WritableLongChunk.makeWritableChunk(chunkSize);
                final WritableObjectChunk<T, Values> prev = WritableObjectChunk.makeWritableChunk(chunkSize);
                final WritableObjectChunk<T, Values> curr = WritableObjectChunk.makeWritableChunk(chunkSize);
                final CloseableIterator<RowSequence> it = ColumnSourceHelper.rowSequenceIterator(rowSet, chunkSize)) {
            // todo: if had shifts, would need to update this code to deal w/ preshift space getModifiedPreShift
            // io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer.doApplyUpdate
            // io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper.KeyedUpdateContext.splitKeyModificationsAndDoKeyChangeRemoves
            while (it.hasNext()) {
                final RowSequence rs = it.next();
                rs.fillRowKeyChunk(keys);
                columnSource.fillPrevChunk(context, prev, rs);
                columnSource.fillChunk(context, curr, rs);
                final int size = keys.size();
                for (int i = 0; i < size; ++i) {
                    if (includePredicate.test(prev.get(i), curr.get(i))) {
                        builder.appendKey(keys.get(i));
                    }
                }
            }
        }
        return builder.build();
    }

    @FunctionalInterface
    interface IntIntPredicate {
        boolean test(int a, int b);

        default IntIntPredicate negate() {
            return (a, b) -> !test(a, b);
        }
    }

    public static RowSet comparePrevChunkToChunkInt(
            ColumnSource<Integer> columnSource,
            RowSet rowSet,
            int chunkSize,
            IntIntPredicate includePredicate) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        try (
                final FillContext context = columnSource.makeFillContext(chunkSize);
                final WritableLongChunk<OrderedRowKeys> keys = WritableLongChunk.makeWritableChunk(chunkSize);
                final WritableIntChunk<Values> prev = WritableIntChunk.makeWritableChunk(chunkSize);
                final WritableIntChunk<Values> curr = WritableIntChunk.makeWritableChunk(chunkSize);
                final CloseableIterator<RowSequence> it = ColumnSourceHelper.rowSequenceIterator(rowSet, chunkSize)) {
            while (it.hasNext()) {
                final RowSequence rs = it.next();
                rs.fillRowKeyChunk(keys);
                columnSource.fillPrevChunk(context, prev, rs);
                columnSource.fillChunk(context, curr, rs);
                final int size = keys.size();
                for (int i = 0; i < size; ++i) {
                    if (includePredicate.test(prev.get(i), curr.get(i))) {
                        builder.appendKey(keys.get(i));
                    }
                }
            }
        }
        return builder.build();
    }
}

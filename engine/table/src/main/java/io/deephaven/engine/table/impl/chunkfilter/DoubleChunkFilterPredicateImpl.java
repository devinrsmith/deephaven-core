package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter.DoubleChunkFilter;

import java.util.Objects;
import java.util.function.DoublePredicate;

final class DoubleChunkFilterPredicateImpl implements DoubleChunkFilter {
    private final DoublePredicate predicate;

    public DoubleChunkFilterPredicateImpl(DoublePredicate predicate) {
        this.predicate = Objects.requireNonNull(predicate);
    }

    @Override
    public void filter(
            DoubleChunk<? extends Values> values,
            LongChunk<OrderedRowKeys> keys,
            WritableLongChunk<OrderedRowKeys> results) {
        results.setSize(0);
        for (int ii = 0; ii < values.size(); ++ii) {
            if (predicate.test(values.get(ii))) {
                results.add(keys.get(ii));
            }
        }
    }
}

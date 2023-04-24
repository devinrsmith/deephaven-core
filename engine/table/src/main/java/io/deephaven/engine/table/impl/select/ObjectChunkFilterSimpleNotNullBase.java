package io.deephaven.engine.table.impl.select;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter.ObjectChunkFilter;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

abstract class ObjectChunkFilterSimpleNotNullBase<T> implements ObjectChunkFilter<T> {

    public ObjectChunkFilter<T> invertFilter() {
        return new InvertedFilter();
    }

    public abstract boolean filter(@NotNull T t);

    @Override
    public final void filter(
            ObjectChunk<T, ? extends Values> values,
            LongChunk<OrderedRowKeys> keys,
            WritableLongChunk<OrderedRowKeys> results) {
        results.setSize(0);
        for (int ix = 0; ix < values.size(); ++ix) {
            final T value = values.get(ix);
            if (filter(value)) {
                results.add(keys.get(ix));
            }
        }
    }

    private class InvertedFilter implements ObjectChunkFilter<T> {
        @Override
        public final void filter(
                ObjectChunk<T, ? extends Values> values,
                LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ix = 0; ix < values.size(); ++ix) {
                final T value = values.get(ix);
                if (value != null && !ObjectChunkFilterSimpleNotNullBase.this.filter(value)) {
                    results.add(keys.get(ix));
                }
            }
        }
    }

    private static class And<T> implements ObjectChunkFilter<T> {

        private final ObjectChunkFilterSimpleNotNullBase<T>[] filters;

        public And(ObjectChunkFilterSimpleNotNullBase<T>[] filters) {
            this.filters = Objects.requireNonNull(filters);
        }

        @Override
        public void filter(
                ObjectChunk<T, ? extends Values> values,
                LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            loop:
            for (int ix = 0; ix < values.size(); ++ix) {
                final T value = values.get(ix);
                if (value == null) {
                    continue;
                }
                for (ObjectChunkFilterSimpleNotNullBase<T> filter : filters) {
                    if (!filter.filter(value)) {
                        continue loop;
                    }
                }
                results.add(keys.get(ix));
            }
        }
    }

    private static class Or<T> implements ObjectChunkFilter<T> {

        private final ObjectChunkFilterSimpleNotNullBase<T>[] filters;

        public Or(ObjectChunkFilterSimpleNotNullBase<T>[] filters) {
            this.filters = Objects.requireNonNull(filters);
        }

        @Override
        public void filter(
                ObjectChunk<T, ? extends Values> values,
                LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ix = 0; ix < values.size(); ++ix) {
                final T value = values.get(ix);
                if (value == null) {
                    continue;
                }
                for (ObjectChunkFilterSimpleNotNullBase<T> filter : filters) {
                    if (filter.filter(value)) {
                        results.add(keys.get(ix));
                        break;
                    }
                }
            }
        }
    }
}

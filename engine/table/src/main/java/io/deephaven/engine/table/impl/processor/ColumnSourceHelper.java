//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequence.Iterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ColumnSource;

import java.util.Objects;

final class ColumnSourceHelper {

    public static CloseableIterator<RowSequence> rowSequenceIterator(RowSet rowSet, int chunkSize) {
        final Iterator it = rowSet.getRowSequenceIterator();
        try {
            return new FixedSizeRowSequenceIterator(it, chunkSize);
        } catch (Throwable t) {
            it.close();
            throw t;
        }
    }

    public static <T> CloseableIterator<ObjectChunk<T, Values>> readFilledChunks(
            ColumnSource<? extends T> columnSource,
            RowSet rowSet,
            boolean usePrev,
            int chunkSize) {
        final FillContext context = columnSource.makeFillContext(chunkSize);
        try {
            final WritableObjectChunk<T, Values> src = WritableObjectChunk.makeWritableChunk(chunkSize);
            try {
                final CloseableIterator<RowSequence> it = rowSequenceIterator(rowSet, chunkSize);
                try {
                    return usePrev
                            ? new ColumnSourceChunkIteratorPrev<>(columnSource, context, src, it)
                            : new ColumnSourceChunkIterator<>(columnSource, context, src, it);
                } catch (Throwable t) {
                    it.close();
                    throw t;
                }
            } catch (Throwable t) {
                src.close();
                throw t;
            }
        } catch (Throwable t) {
            context.close();
            throw t;
        }
    }

    private static final class ColumnSourceChunkIterator<T> extends ColumnSourceChunkIteratorBase<T> {
        public ColumnSourceChunkIterator(ColumnSource<? extends T> columnSource, FillContext context,
                WritableObjectChunk<T, Values> src, CloseableIterator<RowSequence> it) {
            super(columnSource, context, src, it);
        }

        @Override
        public void fillImpl(RowSequence seq) {
            columnSource.fillChunk(context, src, seq);
        }
    }

    private static final class ColumnSourceChunkIteratorPrev<T> extends ColumnSourceChunkIteratorBase<T> {
        public ColumnSourceChunkIteratorPrev(ColumnSource<? extends T> columnSource, FillContext context,
                WritableObjectChunk<T, Values> src, CloseableIterator<RowSequence> it) {
            super(columnSource, context, src, it);
        }

        @Override
        public void fillImpl(RowSequence seq) {
            columnSource.fillPrevChunk(context, src, seq);
        }
    }

    private static abstract class ColumnSourceChunkIteratorBase<T>
            implements CloseableIterator<ObjectChunk<T, Values>> {
        final ColumnSource<? extends T> columnSource;
        final FillContext context;
        final WritableObjectChunk<T, Values> src;
        private final CloseableIterator<RowSequence> it;

        private ColumnSourceChunkIteratorBase(ColumnSource<? extends T> columnSource, FillContext context,
                WritableObjectChunk<T, Values> src, CloseableIterator<RowSequence> it) {
            this.columnSource = Objects.requireNonNull(columnSource);
            this.context = Objects.requireNonNull(context);
            this.src = Objects.requireNonNull(src);
            this.it = Objects.requireNonNull(it);
        }

        public abstract void fillImpl(RowSequence seq);

        @Override
        public final boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public final ObjectChunk<T, Values> next() {
            fillImpl(it.next());
            return src;
        }

        @Override
        public final void close() {
            // noinspection EmptyTryBlock,unused
            try (
                    final FillContext _context = this.context;
                    final WritableObjectChunk<?, ?> _src = this.src;
                    final CloseableIterator<RowSequence> _it = this.it) {
            }
        }
    }

    private static final class FixedSizeRowSequenceIterator implements CloseableIterator<RowSequence> {
        private final Iterator it;
        private final int chunkSize;

        private FixedSizeRowSequenceIterator(Iterator it, int chunkSize) {
            this.it = Objects.requireNonNull(it);
            this.chunkSize = chunkSize;
        }

        @Override
        public boolean hasNext() {
            return it.hasMore();
        }

        @Override
        public RowSequence next() {
            return it.getNextRowSequenceWithLength(chunkSize);
        }

        @Override
        public void close() {
            it.close();
        }
    }
}

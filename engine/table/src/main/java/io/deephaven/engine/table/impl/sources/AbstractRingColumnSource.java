package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.util.datastructures.LongRangeConsumer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.function.LongConsumer;

public abstract class AbstractRingColumnSource<T> extends AbstractColumnSource<T> {

    private final int n;

    public AbstractRingColumnSource(@NotNull Class<T> type) {
        super(type);
    }

    public AbstractRingColumnSource(@NotNull Class<T> type, @Nullable Class<?> elementType) {
        super(type, elementType);
    }

    public final int n() {
        return n;
    }

    abstract Object ring();

    abstract Object prevRing();

    @Override
    public Chunk<Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        final int firstIx = (int) (firstKey % n);
        final int lastIx = (int) (lastKey % n);
        if (firstIx <= lastIx) {
            // Easy case, simple view.
            // More efficient than DefaultGetContext.resetChunkFromArray.
            return DefaultGetContext
                    .getResettableChunk(context)
                    .resetFromArray(ring(), firstIx, (lastIx - firstIx) + 1);
        } else {
            // Would be awesome if we could have a view of two wrapped DoubleChunks
            // final DoubleChunk<Any> c1 = DoubleChunk.chunkWrap(buffer, firstIx, buffer.length - firstIx);
            // final DoubleChunk<Any> c2 = DoubleChunk.chunkWrap(buffer, 0, lastIx + 1);
            // return view(c1, c2);
            final WritableChunk<Values> chunk = DefaultGetContext.getWritableChunk(context);
            final int len = fillChunk(chunk, 0, firstIx, lastIx);
            chunk.setSize(len);
            return chunk;
        }
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            final KeyFiller filler = new KeyFiller(destination);
            rowSequence.forAllRowKeys(filler);
            destination.setSize(filler.destOffset);
        } else {
            final RangeFiller filler = new RangeFiller(destination);
            rowSequence.forAllRowKeyRanges(filler);
            destination.setSize(filler.destOffset);
        }
    }

    private class KeyFiller implements LongConsumer {
        private final WritableChunk<? super Values> destination;
        private int destOffset;

        public KeyFiller(WritableChunk<? super Values> destination) {
            this.destination = Objects.requireNonNull(destination);
        }

        @Override
        public void accept(long key) {
            final int ix = (int) (key % n);


            /*
                    final WritableDoubleChunk<? super Values> typedDest = dest.asWritableDoubleChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllRowKeys(v -> {
            typedDest.set(destPos.intValue(), src.getDouble(v));
            destPos.increment();
        });
        typedDest.setSize(destPos.intValue());
             */
        }
    }

    private class RangeFiller implements LongRangeConsumer {
        private final WritableChunk<? super Values> destination;
        private int destOffset;

        public RangeFiller(WritableChunk<? super Values> destination) {
            this.destination = Objects.requireNonNull(destination);
        }

        @Override
        public void accept(long firstKey, long lastKey) {
            final int firstIx = (int) (firstKey % n);
            final int lastIx = (int) (lastKey % n);
            final int len = fillChunk(destination, destOffset, firstIx, lastIx);
            destOffset += len;
        }
    }

    private int fillChunk(@NotNull WritableChunk<? super Values> destination, int destOffset, int firstIx, int lastIx) {
        if (firstIx <= lastIx) {
            final int len = (lastIx - firstIx) + 1;
            destination.copyFromArray(ring(), firstIx, destOffset, len);
            return len;
        } else {
            final int fill1Length = n - firstIx;
            final int fill2Length = lastIx + 1;
            destination.copyFromArray(ring(), firstIx, destOffset, fill1Length);
            destination.copyFromArray(ring(), 0, destOffset + fill1Length, fill2Length);
            return fill1Length + fill2Length;
        }
    }

    private Chunk<Values> getContiguous(@NotNull GetContext context, int ix, int len) {
        // More efficient than DefaultGetContext.resetChunkFromArray.
        return DefaultGetContext
                .getResettableChunk(context)
                .resetFromArray(ring(), ix, len);
    }

    private void _fillChunk(@NotNull GetContext context, @NotNull WritableChunk<? super Values> destination, long firstKey, long lastKey) {
        final int firstIx = (int) (firstKey % n);
        final int lastIx = (int) (lastKey % n);

    }

    private void _fillChunki(@NotNull GetContext context, @NotNull WritableChunk<? super Values> destination, int firstIx, int lastIx) {




    }
}

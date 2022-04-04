package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ChunkSource.GetContext;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableDoubleArraySource;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RingChunkSourceTest {

    private static ChunkSource<? extends Values> of(int size) {
        double[] doubles = new double[size];
        for (int i = 0; i < size; ++i) {
            doubles[i] = i;
        }
        return new ImmutableDoubleArraySource(doubles);
    }

    private static ChunkSource<? extends Values> SRC = of(1024);

    @Test
    public void empty() {
        final DoubleRingChunkSource ring = new DoubleRingChunkSource(16);
        checkSize(ring, 0);
    }

    @Test
    public void name() {
        final DoubleRingChunkSource ring = new DoubleRingChunkSource(16);
        append(ring, 0, 0);
        checkSize(ring, 1);
    }

    private void append(DoubleRingChunkSource chunk, long firstKey, long lastKey) {
        final int size = (int)(lastKey - firstKey + 1);
        try (
                final FillContext fillContext = chunk.makeFillContext(size);
                final GetContext getContext = chunk.makeGetContext(size)) {
            chunk.append(SRC, fillContext, getContext, firstKey, lastKey);
        }
    }

    private static void checkSize(AbstractRingChunkSource<?, ?, ?> src, int size) {
        if (size == 0) {
            assertThat(src.isEmpty()).isTrue();
            assertThat(src.size()).isZero();
            assertThat(src.firstKey()).isZero();
            assertThat(src.lastKey()).isEqualTo(-1);
        } else {
            assertThat(src.isEmpty()).isTrue();
            assertThat(src.size()).isEqualTo(size);
            assertThat(src.lastKey() - src.firstKey() + 1).isEqualTo(size);
        }
    }

    private Chunk<Values> getChunk(AbstractRingChunkSource<?, ?, ?> src, GetContext context, RowSequence sequence) {
        return src.getChunk(context, sequence);
    }

    private void checkGetChunk(DoubleRingChunkSource ring, RowSequence sequence) {
        final int size = sequence.intSize();
        try (final GetContext getContext = ring.makeGetContext(size)) {
            final Chunk<Values> chunk = ring.getChunk(getContext, sequence);



        }
    }

    private static void checkEquals(Chunk<?> actual, Chunk<?> expected) {
        final int size = actual.size();
        assertThat(size).isEqualTo(expected.size());
        double[] actualArray = new double[size];
        double[] expectedArray = new double[size];
        actual.asDoubleChunk().copyToTypedArray(0, actualArray, 0, size);
        expected.asDoubleChunk().copyToTypedArray(0, expectedArray, 0, size);
        assertThat(actualArray).containsExactly(expectedArray);
    }
}

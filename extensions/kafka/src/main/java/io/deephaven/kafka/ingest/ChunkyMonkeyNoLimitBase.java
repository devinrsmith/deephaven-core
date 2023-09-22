package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;

import java.util.List;

/**
 * A base for no limit ...
 *
 * @param <T> the object type
 */
public abstract class ChunkyMonkeyNoLimitBase<T> implements ChunkyMonkey1<T> {

    @Override
    public final int rowLimit() {
        return Integer.MAX_VALUE;
    }

    @Override
    public final void splay(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        splayAll(in, out);
    }
}

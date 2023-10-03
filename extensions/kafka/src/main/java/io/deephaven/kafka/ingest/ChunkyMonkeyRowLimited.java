package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;

import java.util.List;

/**
 * A specialization of {@link ChunkyMonkey} that provides row-limited guarantees around the implementation of
 * {@link #splayAll(ObjectChunk, List)}.
 *
 * <p>
 * Typically created via extending {@link ChunkyMonkeySingleRowBase} or by limiting an existing implementation via
 * {@link ChunkyMonkey#rowLimit(ChunkyMonkey, int)}.
 *
 * @param <T> the object type
 */
public interface ChunkyMonkeyRowLimited<T> extends ChunkyMonkey<T> {

    /**
     * A guarantee that {@link #splayAll(ObjectChunk, List)} operates on at most row-limit rows at a time.
     *
     * @return the row-limit
     */
    int rowLimit();
}

package io.deephaven.chunk;

import java.util.List;

/**
 * A specialization of {@link ObjectChunksOneToOne} that provides row-limited guarantees around the implementation of
 * {@link #splayAll(ObjectChunk, List)}.
 *
 * @param <T> the object type
 */
public interface ObjectChunksOneToOneRowLimited<T> extends ObjectChunksOneToOne<T> {

    /**
     * A guarantee that {@link #splayAll(ObjectChunk, List)} operates on at most row-limit rows at a time.
     *
     * @return the row-limit
     */
    int rowLimit();
}
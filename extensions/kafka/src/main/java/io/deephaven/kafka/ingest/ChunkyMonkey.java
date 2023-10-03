/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;

/**
 * An interface for splaying data from one or more input objects into output chunks on a 1-to-1 input to output basis.
 *
 * <p>
 * Row-oriented implementations are advised to extend from {@link ChunkyMonkeySingleRowBase}; otherwise extend from
 * {@link ChunkyMonkey} and limit via {@link #rowLimit(ChunkyMonkey, int)} if necessary.
 *
 * @param <T> the object type
 */
public interface ChunkyMonkey<T> {

    /**
     * Creates an implementation where {@link #splayAll(ObjectChunk, List)} operates on at most {@code maxChunkSize}
     * input objects at a time.
     *
     * <p>
     * Adding a row-limit may be useful in cases where the input objects are "wide". By limiting the number of rows
     * considered at any given time, there may be better opportunity for read caching.
     *
     * @param delegate the delegate
     * @param maxChunkSize the max chunk size
     * @return the implementation
     * @param <T> the object type
     */
    static <T> ChunkyMonkeyRowLimited<T> rowLimit(ChunkyMonkey<T> delegate, int maxChunkSize) {
        if (delegate instanceof ChunkyMonkeyRowLimited) {
            final ChunkyMonkeyRowLimited<T> limited = (ChunkyMonkeyRowLimited<T>) delegate;
            if (limited.rowLimit() <= maxChunkSize) {
                // already limited greater than maxChunkSize
                return limited;
            }
            if (limited instanceof ChunkyMonkeyLimiter) {
                // don't want to wrap multiple times, so extract the inner delegate
                delegate = ((ChunkyMonkeyLimiter<T>) limited).delegate();
            }
        }
        return new ChunkyMonkeyLimiter<>(delegate, maxChunkSize);
    }

    /**
     * The logical output types {@code this} instance splays. The size and types directly correspond to the expected
     * size and {@link io.deephaven.chunk.ChunkType chunk types} for {@link #splayAll(ObjectChunk, List)} as specified
     * by {@link ChunkyMonkeyTypes}.
     *
     * @return the output types
     */
    List<Type<?>> outputTypes();

    /**
     * Splays {@code in} into {@code out} by appending {@code in.size()} values to each chunk. The size of each
     * {@code out} chunk will be incremented by {@code in.size()}. Implementations are free to splay the data in a
     * row-oriented, column-oriented, or mix-oriented fashion.
     *
     * <p>
     * If an exception thrown, either due to the logic of the implementation, or in callers' breaking of the contract
     * for {@code out}, the output chunks will be in an unspecified state.
     *
     * @param in the input objects
     * @param out the output chunks as specified by {@link #outputTypes()}; each chunk must have remaining capacity of
     *        at least {@code in.size()}
     */
    void splayAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out);
}

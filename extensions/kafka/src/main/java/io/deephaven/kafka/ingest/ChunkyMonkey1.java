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
 * Row-oriented implementations are advised to extend from {@link ChunkyMonkeyRowBased}; otherwise extend from
 * {@link ChunkyMonkeyNoLimitBase} and limit via {@link #rowLimit(ChunkyMonkey1, int)} if necessary.
 *
 * @param <T> the object type
 */
public interface ChunkyMonkey1<T> {

    /**
     * Creates an implementation where {@link #splayAll(ObjectChunk, List)} operates on at most {@code maxChunkSize}
     * input objects at a time. If {@code maxChunkSize == 1} or {@code delegate instanceof ChunkyMonkeyRowBased}, this
     * will return a row-oriented implementation based off of {@code delegate's} {@link #splay(Object, List)}. If
     * {@code maxChunkSize > 1 && maxChunkSize < Integer.MAX_VALUE}, this will result in a mix-oriented implementation.
     * If {@code maxChunkSize == Integer.MAX_VALUE}, this will return {@code delegate}.
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
    static <T> ChunkyMonkey1<T> rowLimit(ChunkyMonkey1<T> delegate, int maxChunkSize) {
        if (maxChunkSize <= 0) {
            throw new IllegalArgumentException("maxChunkSize must be positive");
        }
        final int delegateRowLimit = delegate.rowLimit();
        if (delegateRowLimit <= maxChunkSize) {
            // already limited
            return delegate;
        }
        if (maxChunkSize == 1) {
            // row-oriented
            return new ChunkyMonkeyByRow<>(delegate);
        }
        if (maxChunkSize < Integer.MAX_VALUE) {
            // mix-oriented
            return new ChunkyMonkeyLimiter<>(delegate, maxChunkSize);
        }
        return delegate;
    }

    /**
     * The logical output types {@code this} instance splays. The size and types directly correspond to the expected
     * size and {@link io.deephaven.chunk.ChunkType chunk types} for {@link #splay(Object, List)},
     * {@link #splay(ObjectChunk, List)}, and {@link #splayAll(ObjectChunk, List)} as specified by
     * {@link ChunkyMonkeyTypes}.
     *
     * @return the output types
     */
    List<Type<?>> outputTypes();

    /**
     * The row-limit {@code this} instance provides with respect to {@link #splay(ObjectChunk, List)}. If unlimited,
     * returns {@link Integer#MAX_VALUE}.
     *
     * @return the row-limit
     */
    int rowLimit();

    /**
     * Splays {@code in} into {@code out} by appending the appropriate value to each chunk. The size of each {@code out}
     * chunk will be incremented by {@code 1}.
     *
     * <p>
     * If an exception thrown, either due to the logic of the implementation, or in callers' breaking of the contract
     * for {@code out}, the output chunks will be in an unspecified state.
     *
     * @param in the input object
     * @param out the output chunks as specified by {@link #outputTypes()}; each chunk must have remaining capacity of
     *        at least {@code 1}
     */
    void splay(T in, List<WritableChunk<?>> out);

    /**
     * Splays {@code in} into {@code out} by appending {@code min(in.size(), rowLimit())} values to each chunk. The size
     * of each {@code out} chunk will be incremented by {@code min(in.size(), rowLimit())}. This is functionally
     * equivalent to calling {@link #splay(Object, List)} with the first {@code in.size()} objects from {@code in}.
     * Implementations are free to splay the data in a row-oriented, column-oriented, or mix-oriented fashion.
     *
     * <p>
     * If an exception thrown, either due to the logic of the implementation, or in callers' breaking of the contract
     * for {@code out}, the output chunks will be in an unspecified state.
     *
     * @param in the input objects
     * @param out the output chunks as specified by {@link #outputTypes()}; each chunk must have remaining capacity of
     *        at least {@code min(in.size(), rowLimit())}
     * @deprecated Should we remove this? extraneous wrt callers just limiting and calling splayAll? That might mean
     *             rowLimit() goes away? Maybe okay if it becomes an implementation detail of splay all.
     */
    void splay(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out);

    /**
     * Splays {@code in} into {@code out} by appending {@code in.size()} values to each chunk. The size of each
     * {@code out} chunk will be incremented by {@code in.size()}. This is functionally equivalent to calling
     * {@link #splay(Object, List)} with each object from {@code in}. Implementations are free to splay the data in a
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

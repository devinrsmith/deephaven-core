/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;

import java.util.List;

/**
 * An interface for splaying data from one or more input objects into output chunks.
 *
 * <p>
 * Row-oriented implementations are advised to extend from {@link ChunkyMonkeyRowBased}.
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
        if (delegate instanceof ChunkyMonkeyRowBased) {
            // row-oriented
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
     * The chunk types {@code this} instance expects.
     *
     * @return the chunk types
     */
    List<ChunkType> chunkTypes();

    /**
     * Splays {@code in} into {@code out} by appending the appropriate value to each chunk. The size of each {@code out}
     * chunk will be incremented by {@code 1}.
     *
     * <p>
     * If there is an exception thrown, either due to the logic of the implementation, or in callers' breaking of the
     * contract for {@code out}, the output state
     *
     * @param in the input object
     * @param out the output chunks, must be exactly the size of and types of {@link #chunkTypes()}, and each chunk must
     *        have remaining capacity of at least {@code 1}
     */
    void splay(T in, List<WritableChunk<?>> out);

    /**
     *
     * @param in
     * @param out
     * @return
     */
    int splay(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out);

    /**
     * Splays {@code in} into {@code out} by appending the appropriate values to each chunk. The size of each
     * {@code out} chunk will be incremented by {@code in.size()}. This is functionally equivalent to calling
     * {@link #splay(Object, List)} with each object from {@code in}. Implementations are free to splay the data in a
     * row-oriented, column-oriented, or mix-oriented fashion.
     *
     * <p>
     * If there is an exception thrown, either due to the logic of the implementation, or in callers' breaking of the
     * contract for {@code out}, the
     *
     * @param in the input objects
     * @param out the output chunks, must be exactly the size of and types of {@link #chunkTypes()}, and each chunk must
     *        have remaining capacity of at least {@code in.size()}
     */
    void splayAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out);
}

/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.chunk;

import io.deephaven.functions.TypedFunction;
import io.deephaven.qst.type.Type;

import java.util.List;

/**
 * An interface for splaying data from one or more input objects into output chunks on a 1-to-1 input to output basis.
 *
 * <p>
 * Row-oriented implementations are advised to extend from {@link ObjectSplayerSingleRowBase}; otherwise extend from
 * {@link ObjectChunksOneToOne} and limit via {@link #rowLimit(ObjectChunksOneToOne, int)} if necessary.
 *
 * @param <T> the object type
 */
public interface ObjectChunksOneToOne<T> {

    /**
     * Creates a function-based splayer whose {@link #outputTypes()} is the {@link TypedFunction#returnType()} from each
     * function in {@code functions}.
     *
     * <p>
     * The implementation of {@link #splayAll(ObjectChunk, List)} is column-oriented with a virtual call and cast
     * per-column.
     *
     * @param functions the functions
     * @return the function splayer
     * @param <T> the object type
     */
    static <T> ObjectChunksOneToOne<T> of(List<TypedFunction<? super T>> functions) {
        return ObjectSplayerFunctionImpl.create(functions);
    }

    /**
     * Creates an implementation based on {@code delegate} where {@link #splayAll(ObjectChunk, List)} operates on at
     * most {@code maxChunkSize} input objects at a time.
     *
     * <p>
     * Adding a row-limit may be useful in cases where the input objects are "wide". By limiting the number of rows
     * considered at any given time, there may be better opportunity for read caching.
     *
     * @param delegate the delegate
     * @param maxChunkSize the max chunk size
     * @return the row-limited splayer
     * @param <T> the object type
     */
    static <T> ObjectChunksOneToOneRowLimited<T> rowLimit(ObjectChunksOneToOne<T> delegate, int maxChunkSize) {
        if (delegate instanceof ObjectChunksOneToOneRowLimited) {
            final ObjectChunksOneToOneRowLimited<T> limited = (ObjectChunksOneToOneRowLimited<T>) delegate;
            if (limited.rowLimit() <= maxChunkSize) {
                // already limited greater than maxChunkSize
                return limited;
            }
            if (limited instanceof ObjectChunksOneToOneRowLimitedImpl) {
                // don't want to wrap multiple times, so operate on the inner delegate
                return rowLimit(((ObjectChunksOneToOneRowLimitedImpl<T>) limited).delegate(), maxChunkSize);
            }
        }
        return new ObjectChunksOneToOneRowLimitedImpl<>(delegate, maxChunkSize);
    }

    /**
     * The logical output types {@code this} instance splays. The size and types correspond to the expected size and
     * {@link io.deephaven.chunk.ChunkType chunk types} for {@link #splayAll(ObjectChunk, List)} as specified by
     * {@link ObjectSplayerTypes}.
     *
     * @return the output types
     */
    List<Type<?>> outputTypes();

    // todo: impls may be threaded? but bad idea?

    /**
     * Splays {@code in} into {@code out} by appending {@code in.size()} values to each chunk. The size of each
     * {@code out} chunk will be incremented by {@code in.size()}. Implementations are free to splay the data in a
     * row-oriented, column-oriented, or mix-oriented fashion. Implementations must not keep any references to the
     * passed-in chunks.
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

    // todo: input is object chunk as really a wrapper around T[], offset, len

    // note: this doesn't have the concept of submitting _less_ than the full chunk
}

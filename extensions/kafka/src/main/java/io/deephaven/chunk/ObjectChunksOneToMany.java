/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProvider.Transaction;
import io.deephaven.qst.type.Type;

import java.util.List;


/**
 * More generalized version of {@link ObjectChunksOneToOne}
 *
 * @param <T>
 */
public interface ObjectChunksOneToMany<T> {

    /**
     * Creates a no-op implementation that consumes all of the input objects without producing any outputs.
     *
     * @param outputTypes the output types
     * @return the no-op implementation
     * @param <T> the object type
     */
    static <T> ObjectChunksOneToMany<T> noop(List<Type<?>> outputTypes) {
        return new ObjectChunksAdapterNoop<>(outputTypes);
    }

    /**
     * todo, clearly document expectations Multiple transactions, one take per transaction. Guarantees the
     * {@link io.deephaven.chunk.ChunksProvider.Transaction#take(int)} will be sized at {@code maxTxSize}, except the
     * last take which may be smaller.
     *
     * <p>
     * Equivalent to {@code of(delegate, maxTxSize, maxTxSize)}.
     *
     */
    static <T> ObjectChunksOneToMany<T> of(ObjectChunksOneToOne<T> delegate, int maxTxSize) {
        return of(delegate, maxTxSize, maxTxSize);
    }

    /**
     * Creates a one-to-one adapter.
     *
     *
     */

    /**
     * Adapts {@code delegate} into a one-to-many interface. This provides structure around how the incoming data is
     * read, and how the outgoing data is shaped.
     *
     * @param delegate the one-to-one delegate
     * @param maxTxSize the maximum transaction size
     * @param maxTakeSize the maximum {@link Transaction#take(int)} size
     * @return
     * @param <T>
     */
    static <T> ObjectChunksOneToMany<T> of(ObjectChunksOneToOne<T> delegate, int maxTxSize, int maxTakeSize) {
        return new ObjectChunksOneToManyAdapter<>(delegate, maxTxSize, maxTakeSize);
    }



    List<Type<?>> outputTypes();

    /**
     *
     * @param inChunks the input objects
     * @param out the chunks provider
     */
    void handleAll(List<? extends ObjectChunk<? extends T, ?>> inChunks, ChunksProvider out);

    // note: this *does* have the concept of submitting _less_ than the full chunk

}

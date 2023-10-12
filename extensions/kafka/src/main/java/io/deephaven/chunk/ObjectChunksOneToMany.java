/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.chunk;

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

    List<Type<?>> outputTypes();

    /**
     *
     * @param in the input objects
     * @param out the chunks provider
     */
    void handleAll(ObjectChunk<? extends T, ?> in, ChunksProvider out);

    // note: this *does* have the concept of submitting _less_ than the full chunk

}

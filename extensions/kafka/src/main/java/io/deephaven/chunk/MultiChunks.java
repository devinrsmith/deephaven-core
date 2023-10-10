/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.chunk;

import io.deephaven.qst.type.Type;

import java.util.List;


public interface MultiChunks<T> {

    /**
     * Creates a no-op implementation that consumes all of the input objects without producing any outputs.
     *
     * @param outputTypes the output types
     * @return the no-op implementation
     * @param <T> the object type
     */
    static <T> MultiChunks<T> noop(List<Type<?>> outputTypes) {
        return new MultiChunksNoOp<>(outputTypes);
    }

    List<Type<?>> outputTypes();

    /**
     *
     * @param in the input objects
     * @param handler the handler
     */
    void handleAll(ObjectChunk<? extends T, ?> in, ChunksProducer handler);

}

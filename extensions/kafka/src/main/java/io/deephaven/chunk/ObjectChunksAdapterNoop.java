package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProvider.Transaction;
import io.deephaven.qst.type.Type;

import java.util.List;

final class ObjectChunksAdapterNoop<T> implements ObjectChunksOneToMany<T> {

    private final List<Type<?>> outputTypes;

    public ObjectChunksAdapterNoop(List<Type<?>> outputTypes) {
        this.outputTypes = List.copyOf(outputTypes);
    }

    @Override
    public List<Type<?>> outputTypes() {
        return outputTypes;
    }

    @Override
    public void handleAll(ObjectChunk<? extends T, ?> in, ChunksProvider out) {
        // todo: do we need to do this since we don't need to relay inRows anymore?
        try (final Transaction tx = out.tx()) {
            tx.submit();
        }
    }
}

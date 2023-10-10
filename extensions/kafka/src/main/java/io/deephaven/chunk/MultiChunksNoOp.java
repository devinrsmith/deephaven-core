package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProducer.Transaction;
import io.deephaven.qst.type.Type;

import java.util.List;

final class MultiChunksNoOp<T> implements MultiChunks<T> {

    private final List<Type<?>> outputTypes;

    public MultiChunksNoOp(List<Type<?>> outputTypes) {
        this.outputTypes = List.copyOf(outputTypes);
    }

    @Override
    public List<Type<?>> outputTypes() {
        return outputTypes;
    }

    @Override
    public void handleAll(ObjectChunk<? extends T, ?> in, ChunksProducer handler) {
        try (final Transaction tx = handler.tx()) {
            tx.commit(in.size());
        }
    }
}

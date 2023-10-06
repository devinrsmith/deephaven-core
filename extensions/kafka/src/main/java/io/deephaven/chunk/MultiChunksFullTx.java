package io.deephaven.chunk;

import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;

public class MultiChunksFullTx<T> implements MultiChunks<T> {
    private final MultiChunks<T> impl;

    public MultiChunksFullTx(MultiChunks<T> impl) {
        this.impl = Objects.requireNonNull(impl);
    }

    @Override
    public List<Type<?>> outputTypes() {
        return impl.outputTypes();
    }

    @Override
    public void handleAll(ObjectChunk<? extends T, ?> in, Handler handler) {
        try (final Transaction rootTx = handler.tx()) {
            final HandlerBatcher batcher = new HandlerBatcher(rootTx);
            impl.handleAll(in, batcher);
            batcher.commitOutstanding();
        }
    }

}

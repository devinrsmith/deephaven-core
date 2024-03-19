/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;

public class OPToNoPImpl<T> implements ObjectProcessorToN<T> {
    private final ObjectProcessor<T> op;

    @Override
    public List<Type<?>> outputTypes() {
        return op.outputTypes();
    }

    @Override
    public void processAll(ObjectChunk<? extends T, ?> in, SinkProvider provider) {

        try (final SinkTx tx = provider.tx()) {
            op.processAll(in, tx.chunks(in.size()));
            tx.commit(in.size());
        }
    }
}

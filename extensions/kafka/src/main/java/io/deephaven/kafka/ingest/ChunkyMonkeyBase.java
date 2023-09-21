package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;

import java.util.List;

public abstract class ChunkyMonkeyBase<T> implements ChunkyMonkey1<T> {

    @Override
    public final void splayAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        final int maxChunkSize = rowLimit().orElse(Integer.MAX_VALUE);
        final int inSize = in.size();
        for (int i = 0; i < inSize; i += maxChunkSize) {
            splay(in.slice(i, Math.min(maxChunkSize, inSize - i)), out);
        }
    }
}

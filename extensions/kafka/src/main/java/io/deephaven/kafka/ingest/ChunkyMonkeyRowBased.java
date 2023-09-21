package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;

import java.util.List;

/**
 * A base for row-oriented implementations. {@link #splayAll(ObjectChunk, List)} is implemented by repeaoted invocations
 * of {@link #splay(Object, List)}.
 *
 * @param <T> the object type
 */
public abstract class ChunkyMonkeyRowBased<T> implements ChunkyMonkey1<T> {

    @Override
    public final int splay(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        splay(in.get(0), out);
        return 1;
    }

    @Override
    public final void splayAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        for (int i = 0; i < in.size(); ++i) {
            splay(in.get(i), out);
        }
    }
}

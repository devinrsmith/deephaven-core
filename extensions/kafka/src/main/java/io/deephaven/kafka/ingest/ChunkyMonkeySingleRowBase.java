package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;

import java.util.List;

/**
 * A specialization that {@link #splayAll(ObjectChunk, List) splays all} one row at a time.
 *
 * <p>
 * In particular, this is a useful construct when {@code T} is a {@link java.nio.ByteBuffer} or {@code byte[]} type and
 * it makes sense to parse straight from bytes into output chunks (as opposed to parsing into a more intermediate state
 * and then splaying into chunks in a column-oriented fashion).
 *
 * @param <T> the object type
 */
public abstract class ChunkyMonkeySingleRowBase<T> implements ChunkyMonkeyRowLimited<T> {

    @Override
    public final int rowLimit() {
        return 1;
    }

    protected abstract void splay(T in, List<WritableChunk<?>> out);

    @Override
    public final void splayAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        for (int i = 0; i < in.size(); ++i) {
            splay(in.get(i), out);
        }
    }
}

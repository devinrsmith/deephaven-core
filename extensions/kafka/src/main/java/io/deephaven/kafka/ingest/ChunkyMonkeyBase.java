package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;

import java.util.List;

public abstract class ChunkyMonkeyBase<T> implements ChunkyMonkey1<T> {

    @Override
    public void splayAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        int remaining = in.size();
        if (remaining == 0) {
            return;
        }
        while (true) {
            final int splayed = splay(in, out);
            if (splayed == remaining) {
                return;
            }
            if (splayed == 0) {
                throw new IllegalStateException("splay on non-empty chunk returned 0");
            }
            if (splayed > remaining) {
                throw new IllegalStateException("splay claims to have splayed more than remaining");
            }
            remaining -= splayed;
            in = in.slice(splayed, remaining);
        }
    }
}

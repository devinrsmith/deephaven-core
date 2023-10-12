package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProvider.Transaction;
import io.deephaven.qst.type.Type;

import java.util.List;

class MultiChunksOtherExample implements ObjectChunksOneToMany<byte[]> {

    @Override
    public List<Type<?>> outputTypes() {
        return List.of(Type.intType());
    }

    @Override
    public void handleAll(List<? extends ObjectChunk<? extends byte[], ?>> inChunks, ChunksProvider out) {
        for (ObjectChunk<? extends byte[], ?> in : inChunks) {
            for (int i = 0; i < in.size(); ++i) {
                // just because we are committing at each row elements does _not_ mean we are handing off to publishing
                // one row at a time; the caller may be batching the chunks as they see fit.
                try (final Transaction tx = out.tx()) {
                    handle(tx, in.get(i));
                    tx.submit();
                }
            }
        }
    }

    private static void handle(Transaction tx, byte[] bytes) {
        final int numInts = bytes.length / Integer.BYTES;
        // If we expect numInts to be large, we should add a layer here that works on some max number of ints at a time.
        final WritableChunks chunks = tx.take(numInts);
        final WritableIntChunk<?> c = chunks.out().get(0).asWritableIntChunk();
        for (int i = 0; i < bytes.length; i += Integer.BYTES) {
            c.add(makeInt(bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]));
        }
        tx.complete(chunks, numInts);
    }

    private static int makeInt(byte i0, byte i1, byte i2, byte i3) {
        return (i0 << 24) | (i1 << 16) | (i2 << 8) | i3;
    }
}

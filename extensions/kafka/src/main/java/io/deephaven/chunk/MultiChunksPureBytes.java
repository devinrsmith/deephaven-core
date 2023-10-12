package io.deephaven.chunk;

import io.deephaven.chunk.ChunksProvider.Transaction;
import io.deephaven.qst.type.Type;

import java.util.List;


/**
 * While this implementation is likely to be of little real-world value, it illustrates the proper implementation hooks
 * needed when the size of the output is known in advanced ({@code outDiscarded == 0}).
 */
class MultiChunksPureBytes implements ObjectChunksOneToMany<byte[]> {

    private final int chunkSize;

    public MultiChunksPureBytes(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Override
    public List<Type<?>> outputTypes() {
        return List.of(Type.byteType());
    }

    @Override
    public void handleAll(List<? extends ObjectChunk<? extends byte[], ?>> inChunks, ChunksProvider out) {
        for (ObjectChunk<? extends byte[], ?> in : inChunks) {
            for (int i = 0; i < in.size(); ++i) {
                try (final Transaction tx = out.tx()) {
                    handle(tx, in.get(i));
                    tx.submit();
                }
            }
        }
    }

    private void handle(Transaction tx, byte[] bytes) {
        for (int i = 0; i < bytes.length && i >= 0; i += chunkSize) {
            final int size = Math.min(chunkSize, bytes.length - i);
            final WritableChunks chunks = tx.take(size);
            {
                final WritableByteChunk<?> out = chunks.out().get(0).asWritableByteChunk();
                out.copyFromTypedArray(bytes, i, chunks.pos(), size);
                out.setSize(chunks.pos() + size);
            }
            tx.complete(chunks, size);
        }
    }
}

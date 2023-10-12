package io.deephaven.chunk;

import java.util.List;

public class ObjectChunksToList<K> {

    private final ChunksProviderBuffered provider;
    private final ObjectChunksOneToManyAdapter<K> splayer;

    public synchronized void handleAll(ObjectChunk<? extends K, ?> in) {
        splayer.handleAll(in, provider);
    }

    public synchronized List<WritableChunks> takeAll() {
        return provider.take();
    }
}

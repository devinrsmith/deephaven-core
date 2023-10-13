package io.deephaven.chunk;

import io.deephaven.util.SafeCloseable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

// this impl is suitable if you know you are always passing reasonably sized in chunks.
class ObjectChunksToListUnbuffered<T> implements ObjectChunkConveyor<T> {

    private static final FactoryImpl FACTORY = new FactoryImpl();

    public static <T> Factory<T> factory() {
        // noinspection unchecked
        return (Factory<T>) FACTORY;
    }

    private static class FactoryImpl implements Factory<Object> {

        @Override
        public ObjectChunkConveyor<Object> of(ObjectChunksOneToOne<Object> delegate, int maxTxSize, int maxTakeSize) {
            return of(ObjectChunksOneToMany.of(delegate, maxTxSize, maxTakeSize), maxTxSize);
        }

        @Override
        public ObjectChunkConveyor<Object> of(ObjectChunksOneToMany<Object> delegate, int desiredChunkSize) {
            return new ObjectChunksToListUnbuffered<>(delegate, desiredChunkSize);
        }
    }

    private static <T> List<ChunkType> types(ObjectChunksOneToMany<T> splayer) {
        return splayer.outputTypes().stream().map(ObjectSplayerTypes::of).collect(Collectors.toList());
    }

    private final ObjectChunksOneToMany<T> splayer;
    private final ChunksProvider provider;
    private List<WritableChunks> out;
    private volatile boolean closed;

    private ObjectChunksToListUnbuffered(ObjectChunksOneToMany<T> splayer, int desiredChunkSize) {
        this.splayer = Objects.requireNonNull(splayer);
        this.provider = ChunksProvider.of(types(splayer), this::acceptChunks, desiredChunkSize);
        this.out = new ArrayList<>();
        this.closed = false;
    }

    @Override
    public void accept(List<? extends ObjectChunk<? extends T, ?>> in) {
        throwIfClosed();
        // Sync happens in #acceptChunks
        splayer.handleAll(in, provider);
    }

    @Override
    public List<WritableChunks> get() {
        throwIfClosed();
        synchronized (this) {
            throwIfClosed();
            try {
                return out;
            } finally {
                out = new ArrayList<>();
            }
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        synchronized (this) {
            if (closed) {
                return;
            }
            try {
                WritableChunks.closeAll(out);
            } finally {
                out = null;
                closed = true;
            }
        }
    }

    private synchronized void acceptChunks(List<? extends WritableChunks> submitted) {
        throwIfClosed();
        out.addAll(submitted);
    }

    private void throwIfClosed() {
        if (closed) {
            throw new IllegalStateException("closed");
        }
    }
}

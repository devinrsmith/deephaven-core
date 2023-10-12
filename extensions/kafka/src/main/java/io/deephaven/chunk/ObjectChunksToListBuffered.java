package io.deephaven.chunk;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class ObjectChunksToListBuffered<T> implements ObjectChunkConveyor<T> {

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
            return new ObjectChunksToListBuffered<>(delegate, ChunksProvider.ofBuffered(types(delegate), desiredChunkSize));
        }
    }

    private static <T> List<ChunkType> types(ObjectChunksOneToMany<T> splayer) {
        return splayer.outputTypes().stream().map(ObjectSplayerTypes::of).collect(Collectors.toList());
    }

    private final ObjectChunksOneToMany<T> splayer;
    private final ChunksProviderBuffered provider;

    private ObjectChunksToListBuffered(ObjectChunksOneToMany<T> splayer, ChunksProviderBuffered provider) {
        this.splayer = Objects.requireNonNull(splayer);
        this.provider = Objects.requireNonNull(provider);
    }

    @Override
    public void accept(List<? extends ObjectChunk<? extends T, ?>> in) {
        splayer.handleAll(in, provider);
    }

    @Override
    public List<WritableChunks> get() {
        return provider.take();
    }

    @Override
    public void close() {
        provider.close();
    }
}

package io.deephaven.chunk;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ObjectChunksOneToOneKV<K, V> {


    public static <K, V> ObjectChunksOneToOneKV<K, V> create(
            ObjectChunksOneToOne<K> key,
            ObjectChunksOneToOne<V> value,
            int keyRowSize,
            int valueRowSize,
            int chunkSize) {
        if (key == null && value == null) {
            throw new IllegalArgumentException();
        }
        if (keyRowSize == 0) {
            keyRowSize = chunkSize;
        }
        if (valueRowSize == 0) {
            valueRowSize = chunkSize;
        }
        if (keyRowSize > chunkSize || chunkSize % keyRowSize != 0) {
            throw new IllegalArgumentException();
        }
        if (valueRowSize > chunkSize || chunkSize % valueRowSize != 0) {
            throw new IllegalArgumentException();
        }
        final List<WritableChunks> keysOut = key == null
                ? null
                : new ArrayList<>();
        final ChunksProvider keyProvider = key == null
                ? null
                : ChunksProvider.of(chunkTypes(key), keysOut::addAll, chunkSize);
        final ObjectChunksOneToMany<K> keyHandler = key == null
                ? null
                : ObjectChunksOneToMany.of(key, keyRowSize);
        final List<WritableChunks> valuesOut = value == null
                ? null
                : new ArrayList<>();
        final ChunksProvider valueProvider = value == null
                ? null
                : ChunksProvider.of(chunkTypes(value), valuesOut::addAll, chunkSize);
        final ObjectChunksOneToMany<V> valueHandler = value == null
                ? null
                : ObjectChunksOneToMany.of(value, valueRowSize);
        return new ObjectChunksOneToOneKV<>(keyProvider, keyHandler, keysOut, valueProvider, valueHandler, valuesOut);
    }

    private static List<ChunkType> chunkTypes(ObjectChunksOneToOne<?> key) {
        return key.outputTypes().stream().map(ObjectSplayerTypes::of).collect(Collectors.toList());
    }

    private final ChunksProvider keyProvider;
    private final ObjectChunksOneToMany<K> keyHandler;
    private final List<? extends WritableChunks> keysOut;

    private final ChunksProvider valueProviders;
    private final ObjectChunksOneToMany<V> valueHandler;
    private final List<? extends WritableChunks> valuesOut;

    private ObjectChunksOneToOneKV(
            ChunksProvider keyProvider,
            ObjectChunksOneToMany<K> keyHandler,
            List<? extends WritableChunks> keysOut,
            ChunksProvider valueProviders,
            ObjectChunksOneToMany<V> valueHandler,
            List<? extends WritableChunks> valuesOut) {
        this.keyProvider = keyProvider;
        this.keyHandler = keyHandler;
        this.keysOut = keysOut;
        this.valueProviders = valueProviders;
        this.valueHandler = valueHandler;
        this.valuesOut = valuesOut;
    }

    public void handle(
            ObjectChunk<? extends K, ?> keys,
            ObjectChunk<? extends V, ?> values) {
        if ((keyHandler == null) != (keys == null)) {
            throw new IllegalArgumentException();
        }
        if ((valueHandler == null) != (values == null)) {
            throw new IllegalArgumentException();
        }
        if (keys != null && values != null) {
            if (keys.size() != values.size()) {
                throw new IllegalArgumentException();
            }
        }
        if (keyHandler != null) {
            keyHandler.handleAll(keys, keyProvider);
        }
        if (valueHandler != null) {
            valueHandler.handleAll(values, valueProviders);
        }
    }

}

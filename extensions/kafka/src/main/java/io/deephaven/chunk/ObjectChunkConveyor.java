package io.deephaven.chunk;

import java.io.Closeable;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface ObjectChunkConveyor<T>
        extends Consumer<List<? extends ObjectChunk<? extends T, ?>>>, Supplier<List<WritableChunks>>, Closeable {

    interface Factory<T> {
        static <T> Factory<T> unbuffered() {
            return ObjectChunksToListUnbuffered.factory();
        }

        static <T> Factory<T> buffered() {
            return ObjectChunksToListBuffered.factory();
        }

        /**
         * Guarantees
         *
         * @param delegate
         * @param maxTxSize
         * @return
         */
        ObjectChunkConveyor<T> of(ObjectChunksOneToOne<T> delegate, int maxTxSize, int maxTakeSize);

        ObjectChunkConveyor<T> of(ObjectChunksOneToMany<T> delegate, int desiredChunkSize);
    }

    @Override
    void accept(List<? extends ObjectChunk<? extends T, ?>> objectChunk);

    @Override
    List<WritableChunks> get();

    @Override
    void close();
}

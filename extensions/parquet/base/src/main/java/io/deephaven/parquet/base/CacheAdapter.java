//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.parquet.compress.CompressorAdapter.Cache;
import io.deephaven.util.channel.SeekableChannelContext;

import java.util.Objects;
import java.util.function.Supplier;

class CacheAdapter implements Cache {
    private final SeekableChannelContext context;

    public CacheAdapter(SeekableChannelContext context) {
        this.context = Objects.requireNonNull(context);
    }

    @Override
    public <T> T get(Key<T> key, Supplier<T> supplier) {
        return context.cache(new KeyAdapter<>(key), supplier);
    }

    private static class KeyAdapter<T> implements SeekableChannelContext.Key<T> {
        private final Key<T> key;

        public KeyAdapter(Key<T> key) {
            this.key = Objects.requireNonNull(key);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            KeyAdapter<?> that = (KeyAdapter<?>) o;
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public String toString() {
            return "KeyAdapter{" +
                    "key=" + key +
                    '}';
        }
    }
}

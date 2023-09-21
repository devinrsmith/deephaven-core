/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.functions.ToShortFunction;
import io.deephaven.kafka.ingest.ChunkUtils;
import io.deephaven.kafka.ingest.ChunkyMonkey1;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

final class ChunkyMonkeyFunctionImpl<T> implements ChunkyMonkey1<T> {

    private final List<Appender<? super T>> appenders = null;


    interface Appender<T> {
        ChunkType chunkType();

        void add(WritableChunk<?> dest, T src);

        void append(WritableChunk<?> dest, ObjectChunk<? extends T, ?> src);
    }

    @Override
    public List<ChunkType> chunkTypes() {
        return appenders.stream().map(Appender::chunkType).collect(Collectors.toList());
    }

    @Override
    public int rowLimit() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void splay(T in, List<WritableChunk<?>> out) {
        checkChunks(out);
        final int L = appenders.size();
        for (int i = 0; i < L; ++i) {
            appenders.get(i).add(out.get(i), in);
        }
    }

    @Override
    public void splay(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        checkChunks(out);
        final int L = appenders.size();
        for (int i = 0; i < L; ++i) {
            appenders.get(i).append(out.get(i), in);
        }
    }

    @Override
    public void splayAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        splay(in, out);
    }

    private void checkChunks(List<WritableChunk<?>> out) {
        if (appenders.size() != out.size()) {
            throw new IllegalArgumentException();
        }
        // we'll catch mismatched chunk types later when we try to cast them
    }

    private static class ShortAppends<T> implements Appender<T> {
        private final ToShortFunction<? super T> f;

        ShortAppends(ToShortFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public ChunkType chunkType() {
            return ChunkType.Short;
        }

        @Override
        public void add(WritableChunk<?> dest, T src) {
            dest.asWritableShortChunk().add(f.applyAsShort(src));
        }

        @Override
        public void append(WritableChunk<?> dest, ObjectChunk<? extends T, ?> src) {
            ChunkUtils.append(dest.asWritableShortChunk(), f, src);
        }
    }
}

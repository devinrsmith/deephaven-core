/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

final class ObjectProcessorCopy<T> implements ObjectProcessor<T> {
    private final GenericType<T> type;

    ObjectProcessorCopy(GenericType<T> type) {
        this.type = Objects.requireNonNull(type);
        if (ObjectProcessor.chunkType(type) != ChunkType.Object) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public List<Type<?>> outputTypes() {
        return Collections.singletonList(type);
    }

    @Override
    public void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        // horrible signature for io.deephaven.chunk.WritableObjectChunk.copyFromTypedChunk
        // that's why we need this hack
        //noinspection unchecked
        copy((ObjectChunk<T, Any>) in, (WritableObjectChunk<T, Any>)out.get(0).asWritableObjectChunk());
    }

    private <ATTR extends Any> void copy(
            ObjectChunk<T, ATTR> src,
            WritableObjectChunk<T, ATTR> dst) {
        final int srcSize = src.size();
        final int dstSize = dst.size();
        dst.copyFromTypedChunk(src, 0, dstSize, srcSize);
        dst.setSize(dstSize + srcSize);
    }
}

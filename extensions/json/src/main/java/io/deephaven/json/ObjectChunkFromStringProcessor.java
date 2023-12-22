/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.json.Function.ToObject;

import java.io.IOException;
import java.util.Objects;

final class ObjectChunkFromStringProcessor<T> extends ObjectChunkBase<T> {

    private final ToObject<? extends T> f;

    public ObjectChunkFromStringProcessor(
            String contextPrefix,
            boolean allowNull,
            boolean allowMissing,
            WritableObjectChunk<? super T, ?> chunk,
            T onNull,
            T onMissing,
            ToObject<? extends T> f) {
        super(contextPrefix, allowNull, allowMissing, chunk, onNull, onMissing);
        this.f = Objects.requireNonNull(f);
    }

    @Override
    protected void handleValueString(JsonParser parser) throws IOException {
        chunk.add(f.apply(parser));
    }
}

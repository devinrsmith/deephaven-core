//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class ObjectEntriesProcessor implements ValueProcessor {
    private final ValueProcessor key;
    private final ValueProcessor value;

    ObjectEntriesProcessor(final ValueProcessor key, final ValueProcessor value) {
        this.key = Objects.requireNonNull(key);
        this.value = Objects.requireNonNull(value);
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        key.processCurrentValue(parser);
        parser.nextToken();
        value.processCurrentValue(parser);
    }

    @Override
    public void processMissing(JsonParser parser) {
        throw new IllegalStateException();
    }

    @Override
    public void setContext(List<WritableChunk<?>> out) {
        final int keySize = key.numColumns();
        key.setContext(out.subList(0, keySize));
        value.setContext(out.subList(keySize, keySize + value.numColumns()));
    }

    @Override
    public void clearContext() {
        key.clearContext();
        value.clearContext();
    }

    @Override
    public int numColumns() {
        return key.numColumns() + value.numColumns();
    }

    @Override
    public Stream<Type<?>> columnTypes() {
        return Stream.concat(key.columnTypes(), value.columnTypes());
    }
}

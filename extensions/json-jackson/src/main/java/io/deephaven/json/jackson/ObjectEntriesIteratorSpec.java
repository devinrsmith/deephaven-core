//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.api.util.NameValidator;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class ObjectEntriesIteratorSpec implements JacksonIteratorSpec {
    private final Mixin<?> key;
    private final Mixin<?> value;

    ObjectEntriesIteratorSpec(final Mixin<?> key, final Mixin<?> value) {
        this.key = Objects.requireNonNull(key);
        this.value = Objects.requireNonNull(value);
    }

    @Override
    public JacksonIterator iterator(JsonParser parser, int chunkCapacity) throws IOException {
        if (parser.isExpectedStartObjectToken()) {
            parser.nextToken();
        }
        final ObjectEntriesProcessor processor = new ObjectEntriesProcessor(
                key.processor("<key>"),
                value.processor("<value>"));
        return new EntriesIterator(processor, parser, chunkCapacity);
    }

    @Override
    public List<Type<?>> outputTypes() {
        return Stream.concat(key.outputTypesImpl(), value.outputTypesImpl()).collect(Collectors.toList());
    }

    @Override
    public int outputSize() {
        return key.outputSize() + value.outputSize();
    }

    @Override
    public List<String> names() {
        return names(Mixin.TO_COLUMN_NAME);
    }

    @Override
    public List<String> names(Function<List<String>, String> f) {
        final Stream<List<String>> keyPath =
                key.outputSize() == 1 && key.paths().findFirst().orElseThrow().isEmpty()
                        ? Stream.of(List.of("Key"))
                        : key.paths();
        final Stream<List<String>> valuePath =
                value.outputSize() == 1 && value.paths().findFirst().orElseThrow().isEmpty()
                        ? Stream.of(List.of("Value"))
                        : value.paths();
        final Stream<List<String>> paths = Stream.concat(keyPath, valuePath);
        return Arrays.asList(NameValidator.legalizeColumnNames(paths.map(f).toArray(String[]::new), true));
    }

    static final class EntriesIterator extends JacksonIterator {

        EntriesIterator(ValueProcessor processor, JsonParser parser, int chunkCapacity) {
            super(processor, parser, chunkCapacity);
            if (!parser.getParsingContext().inObject()) {
                throw new IllegalArgumentException(String.format("Expected to be in an object. ptr @ '%s'",
                        parser.getParsingContext().pathAsPointer()));
            }
        }

        @Override
        public boolean hasNext() {
            return parser.currentToken() != JsonToken.END_OBJECT;
        }
    }
}

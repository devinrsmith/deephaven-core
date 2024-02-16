/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.LocalDateOptions;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class LocalDateMixin extends Mixin {
    private final LocalDateOptions options;

    public LocalDateMixin(LocalDateOptions options, JsonFactory factory) {
        super(factory);
        this.options = Objects.requireNonNull(options);
    }

    @Override
    public int outputCount() {
        return 1;
    }

    @Override
    public Stream<List<String>> paths() {
        return Stream.of(List.of());
    }

    @Override
    public Stream<Type<?>> outputTypes() {
        return Stream.of(Type.ofCustom(LocalDate.class));
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new ObjectValueProcessor<>(out.get(0).asWritableObjectChunk(), new Impl());
    }

    private LocalDate parseString(JsonParser parser) throws IOException {
        final TemporalAccessor accessor = options.dateTimeFormatter().parse(Helpers.textAsCharSequence(parser));
        return LocalDate.from(accessor);
    }

    private LocalDate parseNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, LocalDate.class);
        }
        return options.onNull().orElse(null);
    }

    private LocalDate parseMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, LocalDate.class);
        }
        return options.onMissing().orElse(null);
    }

    private class Impl implements ObjectValueProcessor.ToObject<LocalDate> {
        @Override
        public LocalDate parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_STRING:
                    return parseString(parser);
                case VALUE_NULL:
                    return parseNull(parser);
            }
            throw Helpers.mismatch(parser, LocalDateOptions.class);
        }

        @Override
        public LocalDate parseMissing(JsonParser parser) throws IOException {
            return LocalDateMixin.this.parseMissing(parser);
        }
    }
}

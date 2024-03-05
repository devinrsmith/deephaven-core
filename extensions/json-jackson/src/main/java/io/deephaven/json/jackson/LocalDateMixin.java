/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.LocalDateOptions;
import io.deephaven.json.jackson.ObjectValueProcessor.ToObject;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.stream.Stream;

final class LocalDateMixin extends Mixin<LocalDateOptions> implements ToObject<LocalDate> {

    public LocalDateMixin(LocalDateOptions options, JsonFactory factory) {
        super(factory, options);
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
        return ObjectValueProcessor.of(out.get(0).asWritableObjectChunk(), this);
    }

    @Override
    ArrayProcessor arrayProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        return new ArrayProcessorObjectImpl<>(out.get(0).asWritableObjectChunk()::add, allowMissing, allowNull, null,
                null, this, LocalDate.class);
    }

    @Override
    public LocalDate parseValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_STRING:
                return parseFromString(parser);
            case VALUE_NULL:
                return parseFromNull(parser);
        }
        throw Helpers.mismatch(parser, LocalDateOptions.class);
    }

    @Override
    public LocalDate parseMissing(JsonParser parser) throws IOException {
        return LocalDateMixin.this.parseFromMissing(parser);
    }

    private LocalDate parseFromString(JsonParser parser) throws IOException {
        final TemporalAccessor accessor = options.dateTimeFormatter().parse(Helpers.textAsCharSequence(parser));
        return LocalDate.from(accessor);
    }

    private LocalDate parseFromNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, LocalDate.class);
        }
        return options.onNull().orElse(null);
    }

    private LocalDate parseFromMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, LocalDate.class);
        }
        return options.onMissing().orElse(null);
    }
}

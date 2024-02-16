/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.InstantOptions;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class InstantMixin extends Mixin {
    private final InstantOptions options;

    public InstantMixin(InstantOptions options, JacksonConfiguration factory) {
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
        return Stream.of(Type.instantType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new LongValueProcessor(out.get(0).asWritableLongChunk(), new ToLongImpl());
    }

    private long parseString(JsonParser parser) throws IOException {
        final TemporalAccessor accessor = options.dateTimeFormatter().parse(Helpers.textAsCharSequence(parser));
        final long epochSeconds = accessor.getLong(ChronoField.INSTANT_SECONDS);
        final int nanoOfSecond = accessor.get(ChronoField.NANO_OF_SECOND);
        // todo: overflow
        // io.deephaven.time.DateTimeUtils.safeComputeNanos
        return epochSeconds * 1_000_000_000L + nanoOfSecond;
    }

    private long parseNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Helpers.mismatch(parser, Instant.class);
        }
        return options.onNullOrDefault();
    }

    private long parseMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Helpers.mismatchMissing(parser, Instant.class);
        }
        return options.onMissingOrDefault();
    }

    private class ToLongImpl implements LongValueProcessor.ToLong {
        @Override
        public long parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_STRING:
                    return parseString(parser);
                case VALUE_NULL:
                    return parseNull(parser);
            }
            throw Helpers.mismatch(parser, Instant.class);
        }

        @Override
        public long parseMissing(JsonParser parser) throws IOException {
            return InstantMixin.this.parseMissing(parser);
        }
    }
}

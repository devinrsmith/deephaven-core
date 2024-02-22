/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.InstantNumberOptions;
import io.deephaven.json.StringNumberFormat;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class InstantNumberMixin extends Mixin {

    private static final BigDecimal MULTIPLICAND_10e9 = BigDecimal.valueOf(1_000_000_000);
    private static final BigDecimal MULTIPLICAND_10e6 = BigDecimal.valueOf(1_000_000);
    private static final BigDecimal MULTIPLICAND_10e3 = BigDecimal.valueOf(1_000);

    private final InstantNumberOptions options;

    public InstantNumberMixin(InstantNumberOptions options, JsonFactory factory) {
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
        return new LongValueProcessor(out.get(0).asWritableLongChunk(), function());
    }

    private LongValueProcessor.ToLong function() {
        switch (options.format()) {
            case EPOCH_SECONDS:
                return new EpochSeconds();
            case EPOCH_MILLIS:
                return new EpochMillis();
            case EPOCH_MICROS:
                return new EpochMicros();
            case EPOCH_NANOS:
                return new EpochNanos();
            default:
                throw new IllegalStateException();
        }
    }

    private abstract class Base implements LongValueProcessor.ToLong {

        abstract long parseNumberInt(JsonParser parser) throws IOException;

        abstract long parseNumberFloat(JsonParser parser) throws IOException;

        abstract long parseStringAsNumberInt(JsonParser parser) throws IOException;

        abstract long parseStringAsNumberFloat(JsonParser parser) throws IOException;

        @Override
        public final long parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_NUMBER_INT:
                    if (!options.allowNumberInt()) {
                        throw Helpers.mismatch(parser, Instant.class);
                    }
                    return parseNumberInt(parser);
                case VALUE_NUMBER_FLOAT:
                    if (!options.allowNumberFloat()) {
                        throw Helpers.mismatch(parser, Instant.class);
                    }
                    return parseNumberFloat(parser);
                case VALUE_STRING:
                    if (!options.allowString()) {
                        throw Helpers.mismatch(parser, Instant.class);
                    }
                    switch (options.stringFormat().orElse(StringNumberFormat.INT)) {
                        case INT:
                            return parseStringAsNumberInt(parser);
                        case FLOAT:
                            return parseStringAsNumberFloat(parser);
                    }
                    throw new IllegalStateException();
                case VALUE_NULL:
                    if (!options.allowNull()) {
                        throw Helpers.mismatch(parser, Instant.class);
                    }
                    return options.onNullOrDefault();
            }
            throw Helpers.mismatch(parser, Instant.class);
        }

        @Override
        public final long parseMissing(JsonParser parser) throws IOException {
            if (!options.allowMissing()) {
                throw Helpers.mismatchMissing(parser, Instant.class);
            }
            return options.onMissingOrDefault();
        }
    }

    // We need to parse w/ BigDecimal in the case of VALUE_NUMBER_FLOAT, otherwise we might lose accuracy
    // jshell> (long)(1703292532.123456789 * 1000000000)
    // $4 ==> 1703292532123456768

    private class EpochSeconds extends Base {
        private long epochNanos(long epochSeconds) {
            // todo overflow
            return epochSeconds * 1_000_000_000;
        }

        private long epochNanos(BigDecimal epochSeconds) {
            // todo overflow
            return epochSeconds.multiply(MULTIPLICAND_10e9).longValue();
        }

        @Override
        long parseNumberInt(JsonParser parser) throws IOException {
            return epochNanos(parser.getLongValue());
        }

        @Override
        long parseNumberFloat(JsonParser parser) throws IOException {
            return epochNanos(parser.getDecimalValue());
        }

        @Override
        long parseStringAsNumberInt(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseStringAsLong(parser));
        }

        @Override
        long parseStringAsNumberFloat(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseStringAsBigDecimal(parser));
        }
    }

    private class EpochMillis extends Base {

        private long epochNanos(long epochMillis) {
            // todo overflow
            return epochMillis * 1_000_000;
        }

        private long epochNanos(BigDecimal epochMillis) {
            // todo overflow
            return epochMillis.multiply(MULTIPLICAND_10e6).longValue();
        }

        @Override
        long parseNumberInt(JsonParser parser) throws IOException {
            return epochNanos(parser.getLongValue());
        }

        @Override
        long parseNumberFloat(JsonParser parser) throws IOException {
            return epochNanos(parser.getDecimalValue());
        }

        @Override
        long parseStringAsNumberInt(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseStringAsLong(parser));
        }

        @Override
        long parseStringAsNumberFloat(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseStringAsBigDecimal(parser));
        }
    }

    private class EpochMicros extends Base {
        private long epochNanos(long epochMicros) {
            // todo overflow
            return epochMicros * 1_000;
        }

        private long epochNanos(BigDecimal epochMicros) {
            // todo overflow
            return epochMicros.multiply(MULTIPLICAND_10e3).longValue();
        }

        @Override
        long parseNumberInt(JsonParser parser) throws IOException {
            return epochNanos(parser.getLongValue());
        }

        @Override
        long parseNumberFloat(JsonParser parser) throws IOException {
            return epochNanos(parser.getDecimalValue());
        }

        @Override
        long parseStringAsNumberInt(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseStringAsLong(parser));
        }

        @Override
        long parseStringAsNumberFloat(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseStringAsBigDecimal(parser));
        }
    }

    private class EpochNanos extends Base {
        @Override
        long parseNumberInt(JsonParser parser) throws IOException {
            return parser.getLongValue();
        }

        @Override
        long parseNumberFloat(JsonParser parser) throws IOException {
            return parser.getLongValue();
        }

        @Override
        long parseStringAsNumberInt(JsonParser parser) throws IOException {
            return Helpers.parseStringAsLong(parser);
        }

        @Override
        long parseStringAsNumberFloat(JsonParser parser) throws IOException {
            return Helpers.parseStringAsBigDecimal(parser).longValue();
        }
    }
}

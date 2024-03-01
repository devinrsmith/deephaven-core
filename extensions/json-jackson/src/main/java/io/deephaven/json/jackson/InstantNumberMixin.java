/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.InstantNumberOptions;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

final class InstantNumberMixin extends Mixin<InstantNumberOptions> {

    public InstantNumberMixin(InstantNumberOptions options, JsonFactory factory) {
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

        abstract long parseStringAssumeInt(JsonParser parser) throws IOException;

        abstract long parseStringAssumeFloat(JsonParser parser) throws IOException;

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
                    return options.allowDecimal()
                            ? parseStringAssumeFloat(parser)
                            : parseStringAssumeInt(parser);
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
    // See InstantNumberOptionsTest

    private class EpochSeconds extends Base {
        private long epochNanos(long epochSeconds) {
            return epochSeconds * 1_000_000_000;
        }

        private long epochNanos(BigDecimal epochSeconds) {
            return epochSeconds.scaleByPowerOfTen(9).longValue();
        }

        @Override
        long parseNumberInt(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseNumberIntAsLong(parser));
        }

        @Override
        long parseNumberFloat(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseNumberFloatAsBigDecimal(parser));
        }

        @Override
        long parseStringAssumeInt(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseStringAsLong(parser));
        }

        @Override
        long parseStringAssumeFloat(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseStringAsBigDecimal(parser));
        }
    }

    private class EpochMillis extends Base {

        private long epochNanos(long epochMillis) {
            return epochMillis * 1_000_000;
        }

        private long epochNanos(BigDecimal epochMillis) {
            return epochMillis.scaleByPowerOfTen(6).longValue();
        }

        @Override
        long parseNumberInt(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseNumberIntAsLong(parser));
        }

        @Override
        long parseNumberFloat(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseNumberFloatAsBigDecimal(parser));
        }

        @Override
        long parseStringAssumeInt(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseStringAsLong(parser));
        }

        @Override
        long parseStringAssumeFloat(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseStringAsBigDecimal(parser));
        }
    }

    private class EpochMicros extends Base {
        private long epochNanos(long epochMicros) {
            return epochMicros * 1_000;
        }

        private long epochNanos(BigDecimal epochMicros) {
            return epochMicros.scaleByPowerOfTen(3).longValue();
        }

        @Override
        long parseNumberInt(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseNumberIntAsLong(parser));
        }

        @Override
        long parseNumberFloat(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseNumberFloatAsBigDecimal(parser));
        }

        @Override
        long parseStringAssumeInt(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseStringAsLong(parser));
        }

        @Override
        long parseStringAssumeFloat(JsonParser parser) throws IOException {
            return epochNanos(Helpers.parseStringAsBigDecimal(parser));
        }
    }

    private class EpochNanos extends Base {
        @Override
        long parseNumberInt(JsonParser parser) throws IOException {
            return Helpers.parseNumberIntAsLong(parser);
        }

        @Override
        long parseNumberFloat(JsonParser parser) throws IOException {
            return Helpers.parseNumberFloatAsBigDecimal(parser).longValue();
        }

        @Override
        long parseStringAssumeInt(JsonParser parser) throws IOException {
            return Helpers.parseStringAsLong(parser);
        }

        @Override
        long parseStringAssumeFloat(JsonParser parser) throws IOException {
            return Helpers.parseStringAsBigDecimal(parser).longValue();
        }
    }
}

/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import io.deephaven.time.DateTimeUtils;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Options to parse an {@link Instant} from numbers.
 */
@Immutable
@BuildableStyle
public abstract class InstantNumberOptions extends ValueOptions {

    private static final BigDecimal MULTIPLICAND_10e9 = BigDecimal.valueOf(1_000_000_000);
    private static final BigDecimal MULTIPLICAND_10e6 = BigDecimal.valueOf(1_000_000);
    private static final BigDecimal MULTIPLICAND_10e3 = BigDecimal.valueOf(1_000);

    public enum Format {
        EPOCH_SECONDS, EPOCH_MILLIS, EPOCH_MICROS, EPOCH_NANOS;

        public InstantNumberOptions standard() {
            return builder().format(this).build();
        }

        public InstantNumberOptions strict() {
            return builder()
                    .format(this)
                    .allowNull(false)
                    .allowMissing(false)
                    .build();
        }

        public InstantNumberOptions lenient() {
            return builder()
                    .format(this)
                    .allowNumberFloat(true)
                    .allowString(StringFormat.FLOAT)
                    .build();
        }
    }

    public enum StringFormat {
        NONE, INT, FLOAT
    }

    public static Builder builder() {
        return ImmutableInstantNumberOptions.builder();
    }

    /**
     * The format to use.
     *
     * @return the format
     */
    public abstract Format format();

    /**
     * By default, is {@code true}.
     *
     * @return
     */
    @Default
    public boolean allowNumberInt() {
        return true;
    }

    /**
     * By default, is {@code false}.
     *
     * @return
     */
    @Default
    public boolean allowNumberFloat() {
        return false;
    }

    /**
     * By default, is {@code false}.
     *
     * @return
     */
    @Default
    public StringFormat allowString() {
        return StringFormat.NONE;
    }

    public abstract Optional<Instant> onNull();

    public abstract Optional<Instant> onMissing();

    public interface Builder extends ValueOptions.Builder<InstantNumberOptions, Builder> {
        Builder format(Format format);

        Builder onNull(Instant onNull);

        Builder onMissing(Instant onMissing);

        Builder allowNumberInt(boolean allowNumberInt);

        Builder allowNumberFloat(boolean allowNumberFloat);

        Builder allowString(StringFormat allowString);
    }

    @Derived
    long onNullOrDefault() {
        return DateTimeUtils.epochNanos(onNull().orElse(null));
    }

    @Derived
    long onMissingOrDefault() {
        return DateTimeUtils.epochNanos(onMissing().orElse(null));
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.instantType());
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new LongValueProcessor(out.get(0).asWritableLongChunk(), function());
    }

    @Check
    final void checkNumberFloatInt() {
        if (allowNumberFloat() && !allowNumberInt()) {
            throw new IllegalArgumentException();
        }
    }

    @Check
    final void checkOnNull() {
        if (!allowNull() && onNull().isPresent()) {
            throw new IllegalArgumentException();
        }
    }

    @Check
    final void checkOnMissing() {
        if (!allowMissing() && onMissing().isPresent()) {
            throw new IllegalArgumentException();
        }
    }

    private LongValueProcessor.ToLong function() {
        switch (format()) {
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
                    if (!allowNumberInt()) {
                        throw Helpers.mismatch(parser, Instant.class);
                    }
                    return parseNumberInt(parser);
                case VALUE_NUMBER_FLOAT:
                    if (!allowNumberFloat()) {
                        throw Helpers.mismatch(parser, Instant.class);
                    }
                    return parseNumberFloat(parser);
                case VALUE_STRING:
                    switch (allowString()) {
                        case NONE:
                            throw Helpers.mismatch(parser, Instant.class);
                        case INT:
                            return parseStringAsNumberInt(parser);
                        case FLOAT:
                            return parseStringAsNumberFloat(parser);
                    }
                    throw new IllegalStateException();
                case VALUE_NULL:
                    if (!allowNull()) {
                        throw Helpers.mismatch(parser, Instant.class);
                    }
                    return onNullOrDefault();
            }
            throw Helpers.mismatch(parser, Instant.class);
        }

        @Override
        public final long parseMissing(JsonParser parser) throws IOException {
            if (!allowMissing()) {
                throw Helpers.mismatchMissing(parser, Instant.class);
            }
            return onMissingOrDefault();
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

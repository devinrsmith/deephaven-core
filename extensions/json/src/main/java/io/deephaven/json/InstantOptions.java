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
import java.lang.Runtime.Version;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class InstantOptions extends ValueOptions {

    private static final Version VERSION_12 = Version.parse("12");

    public static Builder builder() {
        return ImmutableInstantOptions.builder();
    }

    public static InstantOptions standard() {
        return builder().build();
    }

    public static InstantOptions strict() {
        return builder().allowNull(false).allowMissing(false).build();
    }

    /**
     * The onNull value to use. Must not set if {@link #allowNull()} is {@code false}.
     *
     * @return
     */
    public abstract Optional<Instant> onNull();

    /**
     * The onMissing value to use. Must not set if {@link #allowMissing()} is {@code false}.
     * 
     * @return
     */
    public abstract Optional<Instant> onMissing();

    /**
     * The date-time formatter to use for {@link DateTimeFormatter#parse(CharSequence) parsing}. The parsed result must
     * support extracting {@link java.time.temporal.ChronoField#INSTANT_SECONDS INSTANT_SECONDS} and
     * {@link java.time.temporal.ChronoField#NANO_OF_SECOND NANO_OF_SECOND} fields. Defaults to
     * {@link DateTimeFormatter#ISO_INSTANT} for java versions 12+, and {@link DateTimeFormatter#ISO_OFFSET_DATE_TIME}
     * otherwise. These defaults will parse offsets, converting to UTC as necessary.
     *
     * @return the date-time formatter
     */
    @Default
    public DateTimeFormatter dateTimeFormatter() {
        // ISO_INSTANT became more versatile in 12+ (handling the parsing of offsets), and is likely more efficient, so
        // we should choose to use it when we can.
        return Runtime.version().compareTo(VERSION_12) >= 0
                ? DateTimeFormatter.ISO_INSTANT
                : DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    }

    public interface Builder extends ValueOptions.Builder<InstantOptions, Builder> {
        Builder onNull(Instant onNull);

        Builder onMissing(Instant onMissing);

        Builder dateTimeFormatter(DateTimeFormatter formatter);

        InstantOptions build();
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.instantType());
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new LongValueProcessor(out.get(0).asWritableLongChunk(), new ToLongImpl());
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

    @Derived
    long onMissingOrDefault() {
        return DateTimeUtils.epochNanos(onMissing().orElse(null));
    }

    @Derived
    long onNullOrDefault() {
        return DateTimeUtils.epochNanos(onNull().orElse(null));
    }

    private long parseString(JsonParser parser) throws IOException {
        final TemporalAccessor accessor = dateTimeFormatter().parse(Helpers.textAsCharSequence(parser));
        final long epochSeconds = accessor.getLong(ChronoField.INSTANT_SECONDS);
        final int nanoOfSecond = accessor.get(ChronoField.NANO_OF_SECOND);
        // todo: overflow
        // io.deephaven.time.DateTimeUtils.safeComputeNanos
        return epochSeconds * 1_000_000_000L + nanoOfSecond;
    }

    private long parseNull(JsonParser parser) throws IOException {
        if (!allowNull()) {
            throw Helpers.mismatch(parser, Instant.class);
        }
        return onNullOrDefault();
    }

    private long parseMissing(JsonParser parser) throws IOException {
        if (!allowMissing()) {
            throw Helpers.mismatchMissing(parser, Instant.class);
        }
        return onMissingOrDefault();
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
            return InstantOptions.this.parseMissing(parser);
        }
    }
}

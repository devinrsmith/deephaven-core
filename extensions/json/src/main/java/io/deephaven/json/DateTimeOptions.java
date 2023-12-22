/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.Function.ToLong;
import io.deephaven.qst.type.Type;
import io.deephaven.time.DateTimeUtils;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.Runtime.Version;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class DateTimeOptions extends SingleValueOptions<Instant, ToLong> {

    private static final Version VERSION_12 = Version.parse("12");

    public static DateTimeOptions of() {
        return builder().build();
    }

    public static Builder builder() {
        return ImmutableDateTimeOptions.builder();
    }

    public long parseString(JsonParser parser, DateTimeFormatter formatter) throws IOException {
        final TemporalAccessor accessor = formatter.parse(Helpers.textAsCharSequence(parser));
        final long epochSeconds = accessor.getLong(ChronoField.INSTANT_SECONDS);
        final int nanoOfSecond = accessor.get(ChronoField.NANO_OF_SECOND);
        // todo: overflow
        // io.deephaven.time.DateTimeUtils.safeComputeNanos
        return epochSeconds * 1_000_000_000L + nanoOfSecond;
    }

    @Override
    @Default
    public boolean allowMissing() {
        return true;
    }

    /**
     * The onMissing value to use. Must not set if {@link #allowMissing()} is {@code false}.
     * 
     * @return
     */
    @Nullable
    public abstract Instant onMissing();


    @Default
    public boolean allowNull() {
        return true;
    }

    /**
     * The onNull value to use. Must not set if {@link #allowNull()} is {@code false}.
     * 
     * @return
     */
    @Nullable
    public abstract Instant onNull();

    private long onMissingOrDefault() {
        return DateTimeUtils.epochNanos(onMissing());
    }

    private long onNullOrDefault() {
        return DateTimeUtils.epochNanos(onNull());
    }

    @Override
    ToLong onValue() {
        final ToLongImpl.Builder builder = ToLongImpl.builder()
                .onString(this::parseString);
        if (allowNull()) {
            builder.onNull(parser -> onNullOrDefault());
        }
        return builder.build();
    }



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

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.instantType());
    }

    private long parseString(JsonParser parser) throws IOException {
        return parseString(parser, dateTimeFormatter());
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new LongImpl(out.get(0).asWritableLongChunk(), onValue(), allowMissing(), onMissingOrDefault());
    }

    @Check
    final void checkOnMissing() {
        if (!allowMissing() && onMissing() != null) {
            throw new IllegalArgumentException();
        }
    }

    public interface Builder extends SingleValueOptions.Builder<Instant, ToLong, DateTimeOptions, Builder> {

        Builder dateTimeFormatter(DateTimeFormatter formatter);
    }
}

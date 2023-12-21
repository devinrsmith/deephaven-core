/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import io.deephaven.time.DateTimeUtils;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class DateTimeOptions extends ValueOptions {


    public static DateTimeOptions of() {
        return builder().build();
    }

    public static Builder builder() {
        return ImmutableDateTimeOptions.builder();
    }

    @Override
    @Default
    public boolean allowNull() {
        return true;
    }

    @Override
    @Default
    public boolean allowMissing() {
        return true;
    }

    public abstract Optional<Instant> onNull();

    public abstract Optional<Instant> onMissing();

    private long onNullOrDefault() {
        return DateTimeUtils.epochNanos(onNull().orElse(null));
    }

    private long onMissingOrDefault() {
        return DateTimeUtils.epochNanos(onMissing().orElse(null));
    }

    /**
     * The date-time formatter to use for {@link DateTimeFormatter#parse(CharSequence) parsing}. The parsed result must
     * support extracting {@link java.time.temporal.ChronoField#INSTANT_SECONDS INSTANT_SECONDS} and
     * {@link java.time.temporal.ChronoField#NANO_OF_SECOND NANO_OF_SECOND} fields. Defaults to
     * {@link DateTimeFormatter#ISO_OFFSET_DATE_TIME}.
     *
     * @return the date-time formatter
     */
    @Default
    public DateTimeFormatter dateTimeFormatter() {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.instantType());
    }

    @Override
    final Set<JsonToken> startTokens() {
        return Set.of(JsonToken.VALUE_STRING);
    }

    private long parse(JsonParser parser) throws IOException {
        final TemporalAccessor accessor = dateTimeFormatter().parse(Helpers.textAsCharSequence(parser));
        final long epochSeconds = accessor.getLong(ChronoField.INSTANT_SECONDS);
        final int nanoOfSecond = accessor.get(ChronoField.NANO_OF_SECOND);
        // todo: overflow
        // io.deephaven.time.DateTimeUtils.safeComputeNanos
        return epochSeconds * 1_000_000_000L + nanoOfSecond;
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new LongChunkFromStringProcessor(context, allowNull(), allowMissing(), out.get(0).asWritableLongChunk(),
                onNullOrDefault(), onMissingOrDefault(), this::parse);
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

    public interface Builder extends ValueOptions.Builder<DateTimeOptions, Builder> {

        Builder dateTimeFormatter(DateTimeFormatter formatter);

        Builder onNull(Instant onNull);

        Builder onMissing(Instant onMissing);
    }
}

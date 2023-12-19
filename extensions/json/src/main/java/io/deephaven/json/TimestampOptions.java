/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class TimestampOptions extends ValueOptions {

    public enum Format {
        DATE_TIME, EPOCH_SECONDS, EPOCH_MILLIS, EPOCH_MICROS, EPOCH_NANOS
    }

    public static TimestampOptions of() {
        return builder().build();
    }

    public static Builder builder() {
        return ImmutableTimestampOptions.builder();
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

    /**
     * The format to use, defaults to {@link Format#DATE_TIME}.
     *
     * @return the format
     */
    @Default
    public Format format() {
        return Format.DATE_TIME;
    }

    /**
     * The date-time formatter to use, only applicable when {@link #format()} is {@link Format#DATE_TIME}. When not
     * specified and {@code format() == DATE_TIME}, defaults to {@link DateTimeFormatter#ISO_OFFSET_DATE_TIME}.
     *
     * @return the date-time formatter
     */
    public abstract Optional<DateTimeFormatter> dateTimeFormatter();

    final DateTimeFormatter dateTimeFormatterOrDefault() {
        return dateTimeFormatter().orElse(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    @Check
    final void checkDateTimeFormatter() {
        if (dateTimeFormatter().isPresent()) {
            if (format() != Format.DATE_TIME) {
                throw new IllegalArgumentException("Only applicable with DATE_TIME format");
            }
        }
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.instantType());
    }

    @Override
    final Map<JsonToken, JsonToken> startEndTokens() {
        return format() == Format.DATE_TIME
                ? Map.of(JsonToken.VALUE_STRING, JsonToken.VALUE_STRING)
                : Map.of(JsonToken.VALUE_NUMBER_INT, JsonToken.VALUE_NUMBER_INT);
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        final Format f = format();
        return f == Format.DATE_TIME
                ? new TimestampProcessor(context, allowNull(), allowMissing(), out.get(0).asWritableLongChunk(),
                        dateTimeFormatterOrDefault())
                : new TimestampIntProcessor(context, allowNull(), allowMissing(), out.get(0).asWritableLongChunk(), f);
    }

    public interface Builder extends ValueOptions.Builder<TimestampOptions, Builder> {
        Builder format(Format format);

        Builder dateTimeFormatter(DateTimeFormatter formatter);
    }
}

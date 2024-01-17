/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class LocalDateOptions extends ValueOptions {
    public static Builder builder() {
        return ImmutableLocalDateOptions.builder();
    }

    public static LocalDateOptions standard() {
        return builder().build();
    }

    public static LocalDateOptions strict() {
        return builder().allowNull(false).allowMissing(false).build();
    }

    /**
     * The date-time formatter to use for {@link DateTimeFormatter#parse(CharSequence) parsing}. The parsed result must
     * support extracting an {@link ChronoField#EPOCH_DAY EPOCH_DAY} field. Defaults to
     * {@link DateTimeFormatter#ISO_LOCAL_DATE}.
     *
     * @return the date-time formatter
     */
    @Default
    public DateTimeFormatter dateTimeFormatter() {
        return DateTimeFormatter.ISO_LOCAL_DATE;
    }

    public abstract Optional<LocalDate> onNull();

    public abstract Optional<LocalDate> onMissing();

    public interface Builder extends ValueOptions.Builder<LocalDateOptions, Builder> {

        Builder dateTimeFormatter(DateTimeFormatter formatter);

        Builder onNull(LocalDate onNull);

        Builder onMissing(LocalDate onMissing);
    }

    @Override
    final int outputCount() {
        return 1;
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.ofCustom(LocalDate.class));
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new ObjectValueProcessor<>(out.get(0).asWritableObjectChunk(), new Impl());
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

    private LocalDate parseString(JsonParser parser) throws IOException {
        final TemporalAccessor accessor = dateTimeFormatter().parse(Helpers.textAsCharSequence(parser));
        return LocalDate.from(accessor);
    }

    private LocalDate parseNull(JsonParser parser) throws IOException {
        if (!allowNull()) {
            throw Helpers.mismatch(parser, LocalDate.class);
        }
        return onNull().orElse(null);
    }

    private LocalDate parseMissing(JsonParser parser) throws IOException {
        if (!allowMissing()) {
            throw Helpers.mismatchMissing(parser, LocalDate.class);
        }
        return onMissing().orElse(null);
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
            return LocalDateOptions.this.parseMissing(parser);
        }
    }
}

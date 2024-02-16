/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Optional;

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

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<LocalDateOptions, Builder> {

        Builder dateTimeFormatter(DateTimeFormatter formatter);

        Builder onNull(LocalDate onNull);

        Builder onMissing(LocalDate onMissing);
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
}

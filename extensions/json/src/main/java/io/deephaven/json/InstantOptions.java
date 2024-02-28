/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.time.DateTimeUtils;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import java.lang.Runtime.Version;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.EnumSet;
import java.util.Set;

/**
 * Processes a JSON string as an {@link Instant}.
 */
@Immutable
@BuildableStyle
public abstract class InstantOptions extends BoxedOptions<Instant> {

    private static final Version VERSION_12 = Version.parse("12");

    public static Builder builder() {
        return ImmutableInstantOptions.builder();
    }

    public static InstantOptions standard() {
        return builder().build();
    }

    public static InstantOptions strict() {
        return builder()
                .allowMissing(false)
                .desiredTypes(JsonValueTypes.STRING.asSet())
                .build();
    }

    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.STRING_OR_NULL;
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

    @Derived
    public long onMissingOrDefault() {
        return DateTimeUtils.epochNanos(onMissing().orElse(null));
    }

    @Derived
    public long onNullOrDefault() {
        return DateTimeUtils.epochNanos(onNull().orElse(null));
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BoxedOptions.Builder<Instant, InstantOptions, Builder> {

        Builder dateTimeFormatter(DateTimeFormatter formatter);
    }

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.STRING_OR_NULL;
    }
}

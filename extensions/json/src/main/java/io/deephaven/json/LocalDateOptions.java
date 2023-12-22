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

    public static LocalDateOptions of() {
        return builder().build();
    }

    public static Builder builder() {
        return null;
        // return ImmutableLocalDateOptions.builder();
    }

    @Override
    @Default
    public boolean allowMissing() {
        return true;
    }

    public abstract Optional<LocalDate> onNull();

    public abstract Optional<LocalDate> onMissing();

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

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.ofCustom(LocalDate.class));
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return null;
        // TODO: consider improving this to long (like we do w/ Instant)
        // return new ObjectChunkFromStringProcessor<>(context, allowNull(), allowMissing(),
        // out.get(0).asWritableObjectChunk(), onNull().orElse(null), onMissing().orElse(null), this::parse);
    }

    // @Check
    // final void checkOnNull() {
    // if (!allowNull() && onNull().isPresent()) {
    // throw new IllegalArgumentException();
    // }
    // }
    //
    // @Check
    // final void checkOnMissing() {
    // if (!allowMissing() && onMissing().isPresent()) {
    // throw new IllegalArgumentException();
    // }
    // }

    private LocalDate parse(JsonParser parser) throws IOException {
        final TemporalAccessor accessor = dateTimeFormatter().parse(Helpers.textAsCharSequence(parser));
        return LocalDate.from(accessor);
    }

    public interface Builder extends ValueOptions.Builder<LocalDateOptions, Builder> {

        Builder dateTimeFormatter(DateTimeFormatter formatter);

        Builder onNull(LocalDate onNull);

        Builder onMissing(LocalDate onMissing);
    }
}

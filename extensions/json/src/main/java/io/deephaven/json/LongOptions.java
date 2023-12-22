/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.Function.ToLong;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class LongOptions extends SingleValueOptions<Long, ToLong> {

    private static final LongOptions STANDARD = builder().build();
    private static final LongOptions STRICT = builder()
            .onValue(ToLongImpl.strict())
            .allowMissing(false)
            .build();
    private static final LongOptions LENIENT = builder()
            .onValue(ToLongImpl.lenient())
            .build();

    public static Builder builder() {
        return ImmutableLongOptions.builder();
    }

    /**
     * The standard Long options, equivalent to {@code builder().build()}.
     *
     * @return the standard Long options
     */
    public static LongOptions standard() {
        return STANDARD;
    }

    /**
     * The strict Long options, equivalent to
     * {@code builder().onValue(ToLongImpl.strict()).allowMissing(false).build()}.
     *
     * @return the strict Long options
     */
    public static LongOptions strict() {
        return STRICT;
    }

    /**
     * The lenient Long options, equivalent to {@code builder().onValue(ToLongImpl.lenient()).build()}.
     *
     * @return the lenient Long options
     */
    public static LongOptions lenient() {
        return LENIENT;
    }

    /**
     * The onValue, defaults to {@link ToLongImpl#standard()}.
     *
     * @return
     */
    @Default
    public ToLong onValue() {
        return ToLongImpl.standard();
    }

    /**
     * If missing values are allowed, defaults to {@code true}.
     *
     * @return allow missing
     */
    @Override
    @Default
    public boolean allowMissing() {
        return true;
    }

    /**
     * The onMissing value to use. Must not set if {@link #allowMissing()} is {@code false}.
     **/
    @Nullable
    public abstract Long onMissing();

    private Long onMissingOrDefault() {
        final Long onMissing = onMissing();
        return onMissing == null ? QueryConstants.NULL_LONG : onMissing;
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.longType());
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new LongImpl(
                out.get(0).asWritableLongChunk(),
                onValue(),
                allowMissing(),
                onMissingOrDefault());
    }

    @Check
    final void checkOnMissing() {
        if (!allowMissing() && onMissing() != null) {
            throw new IllegalArgumentException();
        }
    }

    public interface Builder extends SingleValueOptions.Builder<Long, ToLong, LongOptions, Builder> {

    }
}

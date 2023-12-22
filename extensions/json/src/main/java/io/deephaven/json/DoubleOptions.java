/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.Function.ToDouble;
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
public abstract class DoubleOptions extends SingleValueOptions<Double, ToDouble> {

    private static final DoubleOptions STANDARD = builder().build();
    private static final DoubleOptions STRICT = builder()
            .onValue(ToDoubleImpl.strict())
            .allowMissing(false)
            .build();
    private static final DoubleOptions LENIENT = builder()
            .onValue(ToDoubleImpl.lenient())
            .build();

    public static Builder builder() {
        return ImmutableDoubleOptions.builder();
    }

    /**
     * The standard double options, equivalent to {@code builder().build()}.
     *
     * @return the standard double options
     */
    public static DoubleOptions standard() {
        return STANDARD;
    }

    /**
     * The strict double options, equivalent to
     * {@code builder().onValue(ToDoubleImpl.strict()).allowMissing(false).build()}.
     *
     * @return the strict double options
     */
    public static DoubleOptions strict() {
        return STRICT;
    }

    /**
     * The lenient double options, equivalent to {@code builder().onValue(ToDoubleImpl.lenient()).build()}.
     *
     * @return the lenient double options
     */
    public static DoubleOptions lenient() {
        return LENIENT;
    }

    /**
     * The onValue, defaults to {@link ToDoubleImpl#standard()}.
     *
     * @return
     */
    @Default
    public ToDouble onValue() {
        return ToDoubleImpl.standard();
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
    public abstract Double onMissing();

    private double onMissingOrDefault() {
        final Double onMissing = onMissing();
        return onMissing == null ? QueryConstants.NULL_DOUBLE : onMissing;
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        return Stream.of(Type.doubleType());
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        // todo: wrapper w/ context
        return new DoubleImpl(
                out.get(0).asWritableDoubleChunk(),
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

    public interface Builder extends SingleValueOptions.Builder<Double, ToDouble, DoubleOptions, Builder> {

    }
}

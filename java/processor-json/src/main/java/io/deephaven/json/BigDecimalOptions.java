/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.math.BigDecimal;
import java.util.Optional;

@Immutable
@BuildableStyle
public abstract class BigDecimalOptions extends ValueOptions {
    private static final BigDecimalOptions STANDARD = builder().build();
    private static final BigDecimalOptions STRICT = builder().allowNull(false).allowMissing(false).build();
    private static final BigDecimalOptions LENIENT = builder().allowString(true).build();

    public static Builder builder() {
        return ImmutableBigDecimalOptions.builder();
    }

    public static BigDecimalOptions standard() {
        return STANDARD;
    }

    public static BigDecimalOptions strict() {
        return STRICT;
    }

    public static BigDecimalOptions lenient() {
        return LENIENT;
    }

    @Default
    public boolean allowNumber() {
        return true;
    }

    @Default
    public boolean allowString() {
        return false;
    }

    public abstract Optional<BigDecimal> onNull();

    public abstract Optional<BigDecimal> onMissing();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<BigDecimalOptions, Builder> {
        Builder allowNumber(boolean allowNumber);

        Builder allowString(boolean allowString);

        Builder onNull(BigDecimal onNull);

        Builder onMissing(BigDecimal onMissing);
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

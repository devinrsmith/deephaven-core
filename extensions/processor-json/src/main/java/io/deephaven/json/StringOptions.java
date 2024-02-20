/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/**
 * Processes a JSON value as a {@link String}.
 */
@Immutable
@BuildableStyle
public abstract class StringOptions extends ValueOptions {
    private static final StringOptions STANDARD = builder().build();
    private static final StringOptions STRICT = builder().build();
    private static final StringOptions LENIENT = builder().build();

    public static Builder builder() {
        return ImmutableStringOptions.builder();
    }


    public static StringOptions standard() {
        return STANDARD;
    }

    public static StringOptions strict() {
        return STRICT;
    }

    public static StringOptions lenient() {
        return LENIENT;
    }

    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.STRING_LIKE;
    }

    public abstract Optional<String> onNull();

    public abstract Optional<String> onMissing();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptions.Builder<StringOptions, Builder> {

        Builder onNull(String onNull);

        Builder onMissing(String onMissing);
    }

    @Override
    StringOptions withMissingSupport() {
        throw new RuntimeException();
//        if (allowMissing()) {
//            return this;
//        }
//        final Builder builder = builder()
//                .allowString(allowString())
//                .allowNumberInt(allowNumberInt())
//                .allowNumberFloat(allowNumberFloat())
//                .allowNull(allowNull())
//                .allowMissing(true);
//        onNull().ifPresent(builder::onNull);
//        // todo: option for onMissing?
//        return builder.build();
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

    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.STRING_LIKE;
    }
}

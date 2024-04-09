//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.EnumSet;
import java.util.Set;

/**
 * Processes a JSON value as a {@code boolean}.
 */
@Immutable
@BuildableStyle
public abstract class BoolOptions extends BoxedOptions<Boolean> {

    public static Builder builder() {
        return ImmutableBoolOptions.builder();
    }

    /**
     * The lenient bool options.
     *
     * @return the lenient bool options
     */
    public static BoolOptions lenient() {
        return builder()
                .allowedTypes(JsonValueTypes.BOOL_LIKE)
                .build();
    }

    /**
     * The standard bool options.
     *
     * @return the standard bool options
     */
    public static BoolOptions standard() {
        return builder().build();
    }

    /**
     * The strict bool options.
     *
     * @return the strict bool options
     */
    public static BoolOptions strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.BOOL)
                .build();
    }

    /**
     * The allowed types. By default is {@link JsonValueTypes#BOOL_OR_NULL}.
     */
    @Default
    @Override
    public EnumSet<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.BOOL_OR_NULL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BoxedOptions.Builder<Boolean, BoolOptions, Builder> {

        Builder onNull(boolean onNull);

        Builder onMissing(boolean onMissing);

        default Builder onNull(Boolean onNull) {
            return onNull((boolean) onNull);
        }

        default Builder onMissing(Boolean onMissing) {
            return onMissing((boolean) onMissing);
        }
    }

    @Override
    final EnumSet<JsonValueTypes> restrictedToTypes() {
        return JsonValueTypes.BOOL_LIKE;
    }
}

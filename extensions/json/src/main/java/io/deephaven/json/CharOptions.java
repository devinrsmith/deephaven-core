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
 * Processes a JSON value as a {@code char}.
 */
@Immutable
@BuildableStyle
public abstract class CharOptions extends BoxedOptions<Character> {

    public static Builder builder() {
        return ImmutableCharOptions.builder();
    }


    /**
     * The standard char options.
     *
     * @return the standard char options
     */
    public static CharOptions standard() {
        return builder().build();
    }

    /**
     * The strict char options.
     *
     * @return the strict char options
     */
    public static CharOptions strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.STRING)
                .build();
    }

    /**
     * The allowed types. By default is {@link JsonValueTypes#STRING_OR_NULL}.
     */
    @Default
    @Override
    public EnumSet<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.STRING_OR_NULL;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends BoxedOptions.Builder<Character, CharOptions, Builder> {

        Builder onNull(char onNull);

        Builder onMissing(char onMissing);

        default Builder onNull(Character onNull) {
            return onNull((char) onNull);
        }

        default Builder onMissing(Character onMissing) {
            return onMissing((char) onMissing);
        }
    }

    @Override
    final EnumSet<JsonValueTypes> restrictedToTypes() {
        return JsonValueTypes.STRING_OR_NULL;
    }
}

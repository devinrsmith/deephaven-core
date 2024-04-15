//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Set;

/**
 * Processes a JSON value as a {@code char}.
 */
@Immutable
@BuildableStyle
public abstract class CharOptions extends ValueOptionsSingleValueBase<Character> {

    public static Builder builder() {
        return ImmutableCharOptions.builder();
    }


    /**
     * The standard char options. Allows missing and accepts {@link JsonValueTypes#stringOrNull()}.
     *
     * @return the standard char options
     */
    public static CharOptions standard() {
        return builder().build();
    }

    /**
     * The strict char options. Disallows missing and accepts {@link JsonValueTypes#string()}.
     *
     * @return the strict char options
     */
    public static CharOptions strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.string())
                .build();
    }

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#stringOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.stringOrNull();
    }

    /**
     * {@inheritDoc}. Is {@link JsonValueTypes#stringOrNull()}.
     */
    @Override
    public final Set<JsonValueTypes> universe() {
        return JsonValueTypes.stringOrNull();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptionsSingleValueBase.Builder<Character, CharOptions, Builder> {

        Builder onNull(char onNull);

        Builder onMissing(char onMissing);

        default Builder onNull(Character onNull) {
            return onNull == null ? this : onNull((char) onNull);
        }

        default Builder onMissing(Character onMissing) {
            return onMissing == null ? this : onMissing((char) onMissing);
        }
    }
}

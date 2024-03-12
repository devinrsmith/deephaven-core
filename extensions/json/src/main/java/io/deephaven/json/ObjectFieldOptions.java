//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Set;
import java.util.TreeSet;

@Immutable
@BuildableStyle
public abstract class ObjectFieldOptions {

    public static Builder builder() {
        return ImmutableObjectFieldOptions.builder();
    }

    /**
     * Creates a case-sensitive field with a single {@code name}. Equivalent to
     * {@code builder().name(name).options(options).build()}.
     *
     * @param name the name
     * @param options the options
     * @return the field options
     */
    public static ObjectFieldOptions of(String name, ValueOptions options) {
        return builder().name(name).options(options).build();
    }

    /**
     * The canonical field name.
     */
    public abstract String name();

    /**
     * The value options.
     */
    public abstract ValueOptions options();

    /**
     * The field name aliases.
     */
    public abstract Set<String> aliases();

    @Default
    public boolean caseInsensitiveMatch() {
        return false;
    }

    @Default
    public RepeatedBehavior repeatedBehavior() {
        return RepeatedBehavior.USE_FIRST;
    }

    public enum RepeatedBehavior {
        /**
         * Throws an error if a repeated field is encountered
         */
        ERROR,

        /**
         * Uses the first field of a given name, ignores the rest
         */
        USE_FIRST,

        // /**
        // * Uses the last field of a given name, ignores the rest. Not currently supported.
        // */
        // USE_LAST
    }

    public interface Builder {
        Builder name(String name);

        Builder options(ValueOptions options);

        Builder addAliases(String element);

        Builder addAliases(String... elements);

        Builder addAllAliases(Iterable<String> elements);

        Builder repeatedBehavior(RepeatedBehavior repeatedBehavior);

        Builder caseInsensitiveMatch(boolean caseInsensitiveMatch);

        ObjectFieldOptions build();
    }

    @Check
    final void checkNonOverlapping() {
        if (caseInsensitiveMatch()) {
            final Set<String> set = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            set.add(name());
            for (String alias : aliases()) {
                if (!set.add(alias)) {
                    throw new IllegalArgumentException(
                            String.format("name and aliases must be non-overlapping, found '%s' overlaps", alias));
                }
            }
        } else {
            if (aliases().contains(name())) {
                throw new IllegalArgumentException(
                        String.format("name and aliases must be non-overlapping, found '%s' overlaps", name()));
            }
        }
    }
}

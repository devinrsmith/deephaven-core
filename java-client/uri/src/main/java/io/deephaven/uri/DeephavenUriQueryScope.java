package io.deephaven.uri;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.nio.file.Path;

/**
 * A Deephaven scope URI.
 *
 * <p>
 * For example, {@code dh://example.com/s/my_global_table}.
 */
@Immutable
@BuildableStyle
public abstract class DeephavenUriQueryScope extends DeephavenUriBase {

    public static Builder builder() {
        return ImmutableDeephavenUriQueryScope.builder();
    }

    public static boolean isMatch(Path path) {
        return !path.isAbsolute() && path.getNameCount() == 2 && QUERY_SCOPE.equals(path.getName(0));
    }

    /**
     * The variable name.
     *
     * @return the variable name
     */
    public abstract String variableName();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    /**
     * The path, equivalent to {@code s/${variableName}}.
     *
     * @return the path
     */
    @Override
    public final Path path() {
        return QUERY_SCOPE.resolve(variableName());
    }

    public interface Builder {

        Builder target(DeephavenTarget target);

        Builder variableName(String variableName);

        default Builder parse(Path path) {
            if (!isMatch(path)) {
                throw new IllegalArgumentException();
            }
            return variableName(path.getName(1).toString());
        }

        DeephavenUriQueryScope build();
    }
}

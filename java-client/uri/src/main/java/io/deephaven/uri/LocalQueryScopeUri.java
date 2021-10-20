package io.deephaven.uri;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * A local Deephaven query scope URI.
 *
 * <p>
 * For example, {@code local:///s/my_global_table}.
 */
@Immutable
@SimpleStyle
public abstract class LocalQueryScopeUri extends LocalUriBase {

    public static final Path QUERY_SCOPE = Paths.get("s");

    public static boolean isMatch(Path path) {
        return !path.isAbsolute() && path.getNameCount() == 2 && QUERY_SCOPE.equals(path.getName(0));
    }

    public static LocalQueryScopeUri of(String variableName) {
        return ImmutableLocalQueryScopeUri.of(variableName);
    }

    public static LocalQueryScopeUri of(Path path) {
        if (!isMatch(path)) {
            throw new IllegalArgumentException();
        }
        return of(path.getName(1).toString());
    }

    /**
     * The variable name.
     *
     * @return the variable name
     */
    @Parameter
    public abstract String variableName();

    @Override
    public final <V extends LocalUri.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    /**
     * The path, equivalent to {@code s/${variableName}}.
     *
     * @return the path
     */
    @Override
    public final Path localPath() {
        return QUERY_SCOPE.resolve(variableName());
    }
}

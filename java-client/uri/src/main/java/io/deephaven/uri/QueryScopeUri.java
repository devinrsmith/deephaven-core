package io.deephaven.uri;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Deephaven query scope URI.
 *
 * <p>
 * For example, {@code dh:///scope/my_table}.
 *
 * @see #of(URI) parsing logic
 */
@Immutable
@SimpleStyle
public abstract class QueryScopeUri extends DeephavenUriBase {

    public static final String SCOPE = "scope";

    public static final Pattern PATH_PATTERN = Pattern.compile("^/scope/(.+)$");

    public static QueryScopeUri of(String variableName) {
        return ImmutableQueryScopeUri.of(variableName);
    }

    public static boolean isWellFormed(URI uri) {
        return UriHelper.isDeephavenLocal(uri) && PATH_PATTERN.matcher(uri.getPath()).matches();
    }

    /**
     * Parses the {@code uri} into a query scope URI. The format looks like {@code dh:///scope/${variableName}}.
     *
     * @param uri the URI
     * @return the query scope URI
     */
    public static QueryScopeUri of(URI uri) {
        if (!isWellFormed(uri)) {
            throw new IllegalArgumentException(String.format("Invalid query scope URI '%s'", uri));
        }
        return fromPath(uri.getPath());
    }

    private static QueryScopeUri fromPath(String path) {
        final Matcher matcher = PATH_PATTERN.matcher(path);
        if (!matcher.matches()) {
            throw new IllegalStateException();
        }
        return of(matcher.group(1));
    }

    /**
     * The variable name.
     *
     * @return the variable name
     */
    @Parameter
    public abstract String variableName();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return String.format("%s:///%s/%s", DeephavenUri.LOCAL_SCHEME, SCOPE, variableName());
    }

    @Check
    final void checkVariableName() {
        if (!UriHelper.isUriSafe(variableName())) {
            throw new IllegalArgumentException(String.format("Invalid variable name '%s'", variableName()));
        }
    }

    static class Remote {

        static boolean isWellFormed(URI uri) {
            return RemoteUri.isValidScheme(uri.getScheme())
                    && UriHelper.isRemotePath(uri)
                    && PATH_PATTERN.matcher(uri.getPath()).matches();
        }

        static RemoteUri of(URI uri) {
            if (!isWellFormed(uri)) {
                throw new IllegalArgumentException();
            }
            final DeephavenTarget target = DeephavenTarget.from(uri);
            return fromPath(uri.getPath()).target(target);
        }

        static String toString(DeephavenTarget target, QueryScopeUri uri) {
            return String.format("%s/%s/%s", target, SCOPE, uri.variableName());
        }
    }
}

package io.deephaven.uri;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Deephaven field URI.
 *
 * <p>
 * For example, {@code dh:///field/my_table}.
 *
 * <p>
 * Note: unlike other URIs, this URI can't be resolved by itself - it must be embedded inside of a {@link RemoteUri},
 * whereby the remote URIs {@link RemoteUri#target() target} host will be used as the application id.
 */
@Immutable
@SimpleStyle
public abstract class FieldUri extends DeephavenUriBase {

    public static final Pattern PATH_PATTERN = Pattern.compile("^/field/(.+)$");

    public static FieldUri of(String fieldName) {
        return ImmutableFieldUri.of(fieldName);
    }

    public static boolean isWellFormed(URI uri) {
        return UriHelper.isDeephavenLocal(uri) && PATH_PATTERN.matcher(uri.getPath()).matches();
    }

    /**
     * Parses the {@code URI} into a field URI. The format looks like {@code dh:///field/${fieldName}}.
     *
     * @param uri the URI
     * @return the field URI
     */
    public static FieldUri of(URI uri) {
        if (!isWellFormed(uri)) {
            throw new IllegalArgumentException(String.format("Invalid field URI '%s'", uri));
        }
        return fromPath(uri.getPath());
    }

    private static FieldUri fromPath(String path) {
        final Matcher matcher = PATH_PATTERN.matcher(path);
        if (!matcher.matches()) {
            throw new IllegalStateException();
        }
        return of(matcher.group(1));
    }

    /**
     * The field name.
     *
     * @return the field name
     */
    @Parameter
    public abstract String fieldName();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return String.format("%s:///%s/%s", DeephavenUri.LOCAL_SCHEME, ApplicationUri.FIELD, fieldName());
    }

    @Check
    final void checkFieldName() {
        if (!UriHelper.isUriSafe(fieldName())) {
            throw new IllegalArgumentException(String.format("Invalid field name '%s'", fieldName()));
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

        static String toString(DeephavenTarget target, FieldUri fieldUri) {
            return String.format("%s/%s/%s", target, ApplicationUri.FIELD, fieldUri.fieldName());
        }
    }
}

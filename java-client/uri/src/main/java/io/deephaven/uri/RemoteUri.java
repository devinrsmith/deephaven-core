package io.deephaven.uri;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A remote Deephaven URI represents a structured link for resolving remote Deephaven resources. Is composed of a
 * {@link #target() target} and remote {@link #uri() uri}.
 *
 * <p>
 * For example, {@code dh://host/scope/my_table}.
 *
 * @see #of(URI) parsing logic
 */
@Immutable
@SimpleStyle
public abstract class RemoteUri extends DeephavenUriBase {

    public static RemoteUri of(DeephavenTarget target, StructuredUri uri) {
        return ImmutableRemoteUri.of(target, uri);
    }

    public static boolean isValidScheme(String scheme) {
        return DeephavenTarget.isValidScheme(scheme);
    }

    public static boolean isWellFormed(URI uri) {
        return ApplicationUri.Remote.isWellFormed(uri)
                || FieldUri.Remote.isWellFormed(uri)
                || QueryScopeUri.Remote.isWellFormed(uri)
                || Proxy.isWellFormed(uri);
    }

    /**
     * Parses the {@code uri} into a remote URI.
     *
     * <p>
     * For Deephaven scheme formats, the format looks the same as the local versions, except with a host specified. For
     * example, {@code dh://host/scope/my_table}.
     *
     * <p>
     * The proxy format is of the form {@code dh://host?uri=${innerUri}}; where {@code innerUri} is the URI to be
     * proxied. When {@code innerUri} is a Deephaven scheme, is does not need to be URL encoded; for example,
     * {@code dh://gateway?uri=dh://host/scope/my_table}. Inner URIs that aren't a Deephaven scheme need to be URL
     * encoded; for example, {@code dh://gateway?uri=parquet%3A%2F%2F%2Fdata%2Ftest.parquet}.
     *
     * @param uri the URI
     * @return the remote URI
     */
    public static RemoteUri of(URI uri) {
        if (ApplicationUri.Remote.isWellFormed(uri)) {
            return ApplicationUri.Remote.of(uri);
        }
        if (FieldUri.Remote.isWellFormed(uri)) {
            return FieldUri.Remote.of(uri);
        }
        if (QueryScopeUri.Remote.isWellFormed(uri)) {
            return QueryScopeUri.Remote.of(uri);
        }
        if (Proxy.isWellFormed(uri)) {
            return Proxy.of(uri);
        }
        throw new IllegalArgumentException(String.format("Invalid remote Deephaven URI '%s'", uri));
    }

    /**
     * The Deephaven target.
     *
     * @return the target
     */
    @Parameter
    public abstract DeephavenTarget target();

    /**
     * The <em>inner</em> URI. As opposed to {@link #toURI()}, which represents {@code this} as a URI.
     *
     * @return the inner URI
     */
    @Parameter
    public abstract StructuredUri uri();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return uri().walk(new ToString()).out();
    }

    private class ToString implements Visitor {
        private String out;

        public String out() {
            return Objects.requireNonNull(out);
        }

        @Override
        public void visit(QueryScopeUri queryScopeUri) {
            out = QueryScopeUri.Remote.toString(target(), queryScopeUri);
        }

        @Override
        public void visit(ApplicationUri applicationUri) {
            out = ApplicationUri.Remote.toString(target(), applicationUri);
        }

        @Override
        public void visit(FieldUri fieldUri) {
            out = FieldUri.Remote.toString(target(), fieldUri);
        }

        @Override
        public void visit(RemoteUri remoteUri) {
            out = Proxy.toString(target(), remoteUri);
        }

        @Override
        public void visit(URI customUri) {
            out = Proxy.toString(target(), customUri);
        }
    }

    static class Proxy {

        static final Pattern QUERY_PATTERN = Pattern.compile("^uri=(.+)$");

        static boolean isWellFormed(URI uri) {
            return isValidScheme(uri.getScheme())
                    && UriHelper.isRemoteQuery(uri)
                    && QUERY_PATTERN.matcher(uri.getQuery()).matches();
        }

        static RemoteUri of(URI uri) {
            if (!isWellFormed(uri)) {
                throw new IllegalArgumentException();
            }
            final Matcher matcher = QUERY_PATTERN.matcher(uri.getQuery());
            if (!matcher.matches()) {
                throw new IllegalStateException();
            }
            final URI innerUri = URI.create(matcher.group(1));
            return RemoteUri.isWellFormed(innerUri) ? RemoteUri.of(DeephavenTarget.from(uri), RemoteUri.of(innerUri))
                    : RemoteUri.of(DeephavenTarget.from(uri), CustomUri.of(innerUri));
        }

        static String toString(DeephavenTarget target, RemoteUri uri) {
            return String.format("%s?uri=%s", target, uri);
        }

        static String toString(DeephavenTarget target, URI uri) {
            final String encoded;
            try {
                encoded = URLEncoder.encode(uri.toString(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            return String.format("%s?uri=%s", target, encoded);
        }
    }
}

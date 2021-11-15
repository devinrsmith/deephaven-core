package io.deephaven.uri;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;

public class UriHelper {
    public static boolean isUriSafe(String part) {
        final String encoded;
        try {
            encoded = URLEncoder.encode(part, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return false;
        }
        return part.equals(encoded);
    }

    public static boolean isValidViaApi(URI uri) {
        return !(QueryScopeUri.isWellFormed(uri)
                || ApplicationUri.isWellFormed(uri)
                || FieldUri.isWellFormed(uri));
    }

    /**
     * A URI is a "Deephaven local" when the scheme is {@value DeephavenUri#LOCAL_SCHEME}, the {@link URI#getPath()
     * path} starts with {@code "/"}, and there are no other URI components.
     *
     * @param uri the uri
     * @return true if {@code uri} is a "Deephaven Local"
     */
    public static boolean isDeephavenLocal(URI uri) {
        return DeephavenUri.LOCAL_SCHEME.equals(uri.getScheme()) && isLocalPath(uri);
    }

    /**
     * A URI is a "local path" when the only components are {@link URI#getScheme() scheme} and {@link URI#getPath()
     * path}; and path starts with {@code "/"}.
     *
     * @param uri the URI
     * @return true if {@code uri} is a "local path"
     */
    public static boolean isLocalPath(URI uri) {
        return uri.getHost() == null
                && !uri.isOpaque()
                && uri.getPath().startsWith("/")
                && uri.getQuery() == null
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
    }

    /**
     * A URI is a "remote path" when the only components are {@link URI#getScheme() scheme}, {@link URI#getHost() host},
     * and {@link URI#getPath() path}; and path starts with {@code "/"}.
     *
     * @param uri the URI
     * @return true if {@code uri} is a "remote path"
     */
    public static boolean isRemotePath(URI uri) {
        return uri.getHost() != null
                && !uri.isOpaque()
                && uri.getPath().startsWith("/")
                && uri.getQuery() == null
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
    }

    /**
     * A URI is a "remote target" when the only components are {@link URI#getScheme() scheme}, {@link URI#getHost()
     * host}, and {@link URI#getPath() path}; and path is empty.
     *
     * @param uri the URI
     * @return true if {@code uri} is a "remote target"
     */
    public static boolean isRemoteTarget(URI uri) {
        return uri.getHost() != null
                && !uri.isOpaque()
                && uri.getPath().isEmpty()
                && uri.getQuery() == null
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
    }

    /**
     * A URI is a "remote query" when the only components are {@link URI#getScheme() scheme}, {@link URI#getHost()
     * host}, {@link URI#getQuery() query}, and {@link URI#getPath() path}; and path is empty.
     *
     * @param uri the URI
     * @return true if {@code uri} is a "remote query"
     */
    public static boolean isRemoteQuery(URI uri) {
        return uri.getHost() != null
                && !uri.isOpaque()
                && uri.getPath().isEmpty()
                && uri.getQuery() != null
                && uri.getUserInfo() == null
                && uri.getFragment() == null;
    }
}

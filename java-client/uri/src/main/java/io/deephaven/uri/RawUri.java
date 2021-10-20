package io.deephaven.uri;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.deephaven.uri.DeephavenTarget.PLAINTEXT_SCHEME;
import static io.deephaven.uri.DeephavenTarget.TLS_SCHEME;
import static io.deephaven.uri.LocalUri.LOCAL_SCHEME;

@Immutable
@SimpleStyle
abstract class RawUri extends ResolvableUriBase {

    public static boolean isValidScheme(String scheme) {
        return scheme != null;
    }

    public static boolean isWellFormed(URI uri) {
        return isValidScheme(uri.getScheme());
    }

    public static RawUri of(URI uri) {
        return ImmutableRawUri.of(uri);
    }

    public static RawUri parse(String scheme, String urlEncodedPath) {
        final String schemeSpecificPart;
        try {
            schemeSpecificPart = URLDecoder.decode(urlEncodedPath, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Path is not a raw URI", e);
        }
        final RawUri uri = of(URI.create(String.format("%s:%s", scheme, schemeSpecificPart)));
        if (!scheme.equals(uri.scheme())) {
            throw new IllegalArgumentException("Invalid uri paths");
        }
        return uri;
    }

    @Parameter
    public abstract URI uri();

    @Override
    public final URI toUri() {
        return uri();
    }

    @Override
    public final String scheme() {
        return uri().getScheme();
    }

    @Override
    public final Path toParts() {
        try {
            return Paths.get(uri().getScheme()).resolve(encodedPath());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    private String encodedPath() throws UnsupportedEncodingException {
        String path = uri().getSchemeSpecificPart();
        if (uri().getFragment() != null) {
            path += "#" + uri().getFragment();
        }
        return URLEncoder.encode(path, "UTF-8");
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(uri());
        return visitor;
    }

    @Override
    public final String toString() {
        return super.toString();
    }

    @Check
    final void checkScheme() {
        switch (scheme()) {
            case TLS_SCHEME:
            case PLAINTEXT_SCHEME:
            case LOCAL_SCHEME:
                throw new IllegalArgumentException();
        }
    }

    @Check
    final void checkEncodedPath() {
        try {
            encodedPath();
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}

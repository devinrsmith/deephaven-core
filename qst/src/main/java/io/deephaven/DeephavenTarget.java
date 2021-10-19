package io.deephaven;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.net.URI;
import java.util.OptionalInt;

@Immutable
@BuildableStyle
public abstract class DeephavenTarget {

    public static final String TLS_SCHEME = "dh";

    public static final String PLAINTEXT_SCHEME = "dh+plain";

    public static final String LOCAL_SCHEME = TLS_SCHEME;

    public static Builder builder() {
        return ImmutableDeephavenTarget.builder();
    }

    public static DeephavenTarget from(URI uri) {
        Builder builder = builder();
        switch (uri.getScheme()) {
            case TLS_SCHEME:
                builder.useTLS(true);
                break;
            case PLAINTEXT_SCHEME:
                builder.useTLS(false);
                break;
            default:
                throw new IllegalArgumentException(String.format("Unexpected scheme '%s'", uri.getScheme()));
        }
        final String host = uri.getHost();
        if (host == null) {
            throw new IllegalArgumentException("Unable to construct target with no host");
        }
        builder.host(host);
        final int port = uri.getPort();
        if (port != -1) {
            builder.port(port);
        }
        return builder.build();
    }

    public abstract String host();

    public abstract OptionalInt port();

    @Default
    public boolean useTLS() {
        return "true".equals(System.getProperty("deephaven.uri.tls", "false"));
    }

    //public abstract Optional<String> userAgent();

    public final String scheme() {
        return useTLS() ? TLS_SCHEME : PLAINTEXT_SCHEME;
    }

    public final String authority() {
        return port().isPresent() ? String.format("%s:%d", host(), port().getAsInt()) : host();
    }

    public final URI targetUri() {
        return URI.create(String.format("%s://%s", scheme(), authority()));
    }

    @Check
    final void checkHostPort() {
        // Will cause URI exception if port is invalid too
        if (!host().equals(targetUri().getHost())) {
            throw new IllegalArgumentException(String.format("Invalid host '%s'", host()));
        }
    }

    public interface Builder {

        Builder host(String host);

        Builder port(int port);

        Builder useTLS(boolean useTLS);

        //Builder userAgent(String userAgent);

        DeephavenTarget build();
    }
}

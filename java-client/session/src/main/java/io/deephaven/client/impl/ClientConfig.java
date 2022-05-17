package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.server.config.SSLConfig;
import io.deephaven.uri.DeephavenTarget;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

@Immutable
@BuildableStyle
public abstract class ClientConfig {

    public static Builder builder() {
        return ImmutableClientConfig.builder();
    }

    public abstract DeephavenTarget target();

    public abstract Optional<SSLConfig> ssl();

    public abstract Optional<String> userAgent();

    /**
     * The maximum inbound message size. Defaults to 100MiB.
     *
     * @return the maximum inbound message size
     */
    @Default
    public int maxInboundMessageSize() {
        return 100 * 1024 * 1024;
    }

    public final SSLConfig sslOrDefault() {
        return ssl().orElseGet(SSLConfig::defaultConfig);
    }

    @Check
    final void checkSslStatus() {
        if (!target().isSecure() && ssl().isPresent()) {
            throw new IllegalArgumentException("target() is trying to connect via plaintext, but ssl() is present");
        }
    }

    public interface Builder {

        Builder target(DeephavenTarget target);

        Builder ssl(SSLConfig ssl);

        Builder userAgent(String userAgent);

        Builder maxInboundMessageSize(int maxInboundMessageSize);

        ClientConfig build();
    }
}

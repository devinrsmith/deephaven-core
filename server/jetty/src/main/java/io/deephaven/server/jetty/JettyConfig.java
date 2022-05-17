package io.deephaven.server.jetty;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.server.config.ServerConfig;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

/**
 * The jetty server configuration.
 */
@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableJettyConfig.class)
public abstract class JettyConfig implements ServerConfig {

    public static final int DEFAULT_SSL_PORT = 443;
    public static final int DEFAULT_PLAINTEXT_PORT = 10000;
    public static final boolean DEFAULT_WITH_WEBSOCKETS = true;

    public static JettyConfig defaultConfig() {
        return ImmutableJettyConfig.builder().build();
    }

    /**
     * The port. Defaults to {@value DEFAULT_SSL_PORT} if {@link #ssl()} is present, otherwise defaults to
     * {@value DEFAULT_PLAINTEXT_PORT}.
     */
    @Default
    public int port() {
        return ssl().isPresent() ? DEFAULT_SSL_PORT : DEFAULT_PLAINTEXT_PORT;
    }

    /**
     * Include websockets. Defaults to {@value DEFAULT_WITH_WEBSOCKETS}.
     */
    @Default
    public boolean websockets() {
        return DEFAULT_WITH_WEBSOCKETS;
    }
}

package io.deephaven.server.netty;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.server.config.ServerConfig;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

/**
 * The netty server configuration.
 */
@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableNettyConfig.class)
public abstract class NettyConfig implements ServerConfig {

    public static final int DEFAULT_SSL_PORT = 443;
    public static final int DEFAULT_PLAINTEXT_PORT = 8080;

    public static NettyConfig defaultConfig() {
        return ImmutableNettyConfig.builder().build();
    }

    /**
     * The port. Defaults to {@value DEFAULT_SSL_PORT} if {@link #ssl()} is present, otherwise defaults to
     * {@value DEFAULT_PLAINTEXT_PORT}.
     */
    @Default
    public int port() {
        return ssl().isPresent() ? DEFAULT_SSL_PORT : DEFAULT_PLAINTEXT_PORT;
    }
}

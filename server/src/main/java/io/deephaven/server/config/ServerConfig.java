package io.deephaven.server.config;

import io.deephaven.ssl.config.SSLConfig;
import org.immutables.value.Value.Default;

import java.time.Duration;
import java.util.Optional;

/**
 * The server configuration.
 */
public interface ServerConfig {

    String DEFAULT_HOST = "0.0.0.0";
    Duration DEFAULT_TOKEN_EXPIRE = Duration.ofMinutes(5);
    int DEFAULT_SCHEDULER_POOL_SIZE = 4;
    int DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 100 * 1024 * 1024;

    /**
     * The host. Defaults to {@value DEFAULT_HOST}.
     */
    @Default
    default String host() {
        return DEFAULT_HOST;
    }

    /**
     * The port.
     */
    int port();

    /**
     * The optional SSL configuration.
     */
    Optional<SSLConfig> ssl();

    /**
     * The token expiration. Defaults to 5 minutes.
     */
    @Default
    default Duration tokenExpire() {
        return DEFAULT_TOKEN_EXPIRE;
    }

    /**
     * The scheduler pool size. Defaults to {@value DEFAULT_SCHEDULER_POOL_SIZE}.
     */
    @Default
    default int schedulerPoolSize() {
        return DEFAULT_SCHEDULER_POOL_SIZE;
    }

    /**
     * The maximum inbound message size. Defaults to 100 MiB.
     */
    @Default
    default int maxInboundMessageSize() {
        return DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
    }
}

package io.deephaven.server.config;

import org.immutables.value.Value.Default;

import java.time.Duration;
import java.util.Optional;

public interface ServerConfig {
    @Default
    default String host() {
        return "0.0.0.0";
    }

    int port();

    Optional<SSLConfig> ssl();

    @Default
    default Duration tokenExpire() {
        return Duration.ofMinutes(5);
    }

    @Default
    default int schedulerPoolSize() {
        return 4;
    }

    @Default
    default int maxInboundMessageSize() {
        return 100 * 1024 * 1024;
    }
}

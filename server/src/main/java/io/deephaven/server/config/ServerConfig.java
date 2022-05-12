package io.deephaven.server.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.util.Optional;

@Immutable
@JsonDeserialize(as = ImmutableServerConfig.class)
public abstract class ServerConfig {
    @Default
    public String host() {
        return "0.0.0.0";
    }

    public abstract int port();

    public abstract Optional<SSLConfig> ssl();

    @Default
    public boolean withWebsockets() {
        return true; // todo; jetty only?
    }

    @Default
    public Duration tokenExpire() {
        return Duration.ofMinutes(5);
    }

    @Default
    public int schedulerPoolSize() {
        return 4;
    }

    @Default
    public int maxInboundMessageSize() {
        return 100 * 1024 * 1024;
    }
}

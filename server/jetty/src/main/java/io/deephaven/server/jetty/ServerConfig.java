package io.deephaven.server.jetty;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

@Immutable
@JsonDeserialize(as = ImmutableServerConfig.class)
public abstract class ServerConfig {

    public abstract int port();

    public abstract Optional<SSLConfig> ssl();

    @Default
    public boolean withWebsockets() {
        return true;
    }

    public final Optional<SslContextFactory.Server> createSslContextFactory() {
        return ssl().map(s -> s.append(new SslContextFactory.Server()));
    }
}

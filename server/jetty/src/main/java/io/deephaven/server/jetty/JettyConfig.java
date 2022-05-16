package io.deephaven.server.jetty;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.server.config.ServerConfig;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableJettyConfig.class)
public abstract class JettyConfig implements ServerConfig {

    public static JettyConfig defaultConfig() {
        return ImmutableJettyConfig.builder().build();
    }

    @Default
    public int port() {
        return ssl().isPresent() ? 443 : 10000;
    }

    @Default
    public boolean withWebsockets() {
        return true;
    }
}

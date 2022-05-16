package io.deephaven.server.netty;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.server.config.ServerConfig;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableNettyConfig.class)
public abstract class NettyConfig implements ServerConfig {

    public static NettyConfig defaultConfig() {
        return ImmutableNettyConfig.builder().build();
    }

    @Default
    public int port() {
        return ssl().isPresent() ? 443 : 8080;
    }
}

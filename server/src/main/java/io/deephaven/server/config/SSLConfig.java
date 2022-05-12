package io.deephaven.server.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

@Immutable
@JsonDeserialize(as = ImmutableSSLConfig.class)
public abstract class SSLConfig {
    public abstract KeySourceConfig key();

    public abstract Optional<KeySourceConfig> trust();
}

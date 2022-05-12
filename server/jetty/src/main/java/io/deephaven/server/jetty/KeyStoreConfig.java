package io.deephaven.server.jetty;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

@Immutable
@JsonDeserialize(as = ImmutableKeyStoreConfig.class)
public abstract class KeyStoreConfig {
    public abstract String path();

    public abstract String password();

    public abstract Optional<String> type();

    public abstract Optional<String> provider();
}

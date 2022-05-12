package io.deephaven.server.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

@Immutable
@JsonDeserialize(as = ImmutableKeyStoreConfig.class)
public abstract class KeyStoreConfig implements KeySourceConfig {
    public abstract String path();

    public abstract String password();

    public abstract Optional<String> type();

    public abstract Optional<String> provider();

    @Override
    public final <V extends Visitor<T>, T> T walk(V visitor) {
        return visitor.visit(this);
    }
}

package io.deephaven.server.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableKeyStoreConfig.class)
public abstract class KeyStoreConfig implements IdentityConfig {
    public abstract String path();

    public abstract String password();

    public abstract Optional<String> keystoreType();

    @Override
    public final <V extends Visitor<T>, T> T walk(V visitor) {
        return visitor.visit(this);
    }
}

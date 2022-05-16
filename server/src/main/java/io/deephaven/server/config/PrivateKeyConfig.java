package io.deephaven.server.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

@Immutable
@JsonDeserialize(as = ImmutablePrivateKeyConfig.class)
public abstract class PrivateKeyConfig implements IdentityConfig {
    public abstract String certChainPath();

    public abstract String privateKeyPath();

    public abstract Optional<String> privateKeyPassword();

    public abstract Optional<String> alias();

    @Override
    public final <V extends Visitor<T>, T> T walk(V visitor) {
        return visitor.visit(this);
    }
}

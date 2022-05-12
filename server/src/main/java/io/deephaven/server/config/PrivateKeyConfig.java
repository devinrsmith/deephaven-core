package io.deephaven.server.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value.Immutable;

@Immutable
@JsonDeserialize(as = ImmutablePrivateKeyConfig.class)
public abstract class PrivateKeyConfig implements KeySourceConfig {
    public abstract String certChainPath();

    public abstract String privateKeyPath();

    @Override
    public final <V extends Visitor<T>, T> T walk(V visitor) {
        return visitor.visit(this);
    }
}

package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableTrustStoreConfig.class)
public abstract class TrustStoreConfig implements TrustConfig {
    public abstract String path();

    public abstract String password();

    @Override
    public final <V extends Visitor<T>, T> T walk(V visitor) {
        return visitor.visit(this);
    }
}

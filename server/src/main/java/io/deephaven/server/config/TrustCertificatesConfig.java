package io.deephaven.server.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value.Immutable;

@Immutable
@JsonDeserialize(as = ImmutableTrustCertificatesConfig.class)
public abstract class TrustCertificatesConfig implements TrustConfig {
    public abstract String path();

    @Override
    public final <V extends Visitor<T>, T> T walk(V visitor) {
        return visitor.visit(this);
    }
}

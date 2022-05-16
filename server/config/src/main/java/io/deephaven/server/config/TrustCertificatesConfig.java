package io.deephaven.server.config;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableTrustCertificatesConfig.class)
public abstract class TrustCertificatesConfig implements TrustConfig {
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    public abstract List<String> path();

    @Override
    public final <V extends Visitor<T>, T> T walk(V visitor) {
        return visitor.visit(this);
    }

    @Check
    final void checkPath() {
        if (path().isEmpty()) {
            throw new IllegalArgumentException("path must have at least one element");
        }
    }
}

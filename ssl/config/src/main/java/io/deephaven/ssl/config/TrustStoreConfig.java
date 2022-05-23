package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

/**
 * The trust material from a trust store.
 */
@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableTrustStoreConfig.class)
public abstract class TrustStoreConfig implements Trust {
    public static TrustStoreConfig of(String path, String password) {
        return ImmutableTrustStoreConfig.builder().path(path).password(password).build();
    }

    /**
     * The trust store path.
     */
    public abstract String path();

    /**
     * The trust storce password.
     */
    public abstract String password();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

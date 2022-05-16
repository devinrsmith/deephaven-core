package io.deephaven.server.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value.Immutable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Immutable
@JsonDeserialize(as = ImmutableSSLConfig.class)
public abstract class SSLConfig {
    public abstract IdentityConfig identity();

    public List<TrustConfig> trust() {
        return Collections.emptyList();
    }

    public boolean withJDKTrust() {
        return true;
    }

    public boolean withSystemPropertyTrust() {
        return false;
    }

    public boolean withTrustAll() {
        return false;
    }
}

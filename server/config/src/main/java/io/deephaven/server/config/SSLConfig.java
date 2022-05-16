package io.deephaven.server.config;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableSSLConfig.class)
public abstract class SSLConfig {

    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    public abstract List<IdentityConfig> identity();

    @Default
    public boolean withSystemPropertyIdentity() {
        return false;
    }

    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    public abstract List<TrustConfig> trust();

    @Default
    public boolean withJDKTrust() {
        return true;
    }

    @Default
    public boolean withSystemPropertyTrust() {
        return false;
    }

    @Default
    public boolean withTrustAll() {
        return false;
    }

    public abstract List<String> ciphers();

    @Default
    public boolean withSystemPropertyCiphers() {
        return false;
    }

    public abstract List<String> protocols();

    @Default
    public boolean withSystemPropertyProtocols() {
        return false;
    }

    @Default
    public ClientAuth clientAuthentication() {
        return ClientAuth.NONE;
    }

    public enum ClientAuth {
        NONE,
        WANTED,
        NEEDED
    }
}

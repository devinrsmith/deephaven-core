package io.deephaven.server.jetty;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

@Immutable
@JsonDeserialize(as = ImmutableSSLConfig.class)
public abstract class SSLConfig {
    public abstract Optional<KeyStoreConfig> trustStore();

    public abstract Optional<KeyStoreConfig> keyStore();

    public final <S extends SslContextFactory> S append(S factory) {
        {
            final KeyStoreConfig trustStore = trustStore().orElse(null);
            if (trustStore != null) {
                factory.setTrustStorePath(trustStore.path());
                factory.setTrustStorePassword(trustStore.password());
                trustStore.type().ifPresent(factory::setTrustStoreType);
                trustStore.provider().ifPresent(factory::setTrustStoreProvider);
            }
        }
        {
            final KeyStoreConfig keyStore = keyStore().orElse(null);
            if (keyStore != null) {
                factory.setKeyStorePath(keyStore.path());
                factory.setKeyStorePassword(keyStore.password());
                keyStore.type().ifPresent(factory::setKeyStoreType);
                keyStore.provider().ifPresent(factory::setKeyStoreProvider);
            }
        }
        return factory;
    }
}

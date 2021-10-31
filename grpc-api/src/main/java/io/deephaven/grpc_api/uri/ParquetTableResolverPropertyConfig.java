package io.deephaven.grpc_api.uri;

import javax.inject.Inject;
import java.net.URI;

public class ParquetTableResolverPropertyConfig implements ParquetTableResolver.Config {

    public static final String PARQUET_TABLE_RESOLVER_ENABLED_KEY = "deephaven.resolver.ParquetTableResolver.enabled";

    /**
     * The {@code true} value.
     */
    public static final String TRUE = "true";

    /**
     * The {@code false} value.
     */
    public static final String FALSE = "false";

    @Inject
    public ParquetTableResolverPropertyConfig() {
        final String expected = UriResolversPropertyConfig.propertyKey(ParquetTableResolver.class);
        if (!PARQUET_TABLE_RESOLVER_ENABLED_KEY.equals(expected)) {
            throw new IllegalStateException(String.format("The GLOBAL_KEY constant '%s' should be updated to '%s'",
                    PARQUET_TABLE_RESOLVER_ENABLED_KEY, expected));
        }
    }

    /**
     * Looks up the property key {@value PARQUET_TABLE_RESOLVER_ENABLED_KEY}, {@code true} when equal to {@value TRUE};
     * {@code false} otherwise.
     *
     * @return {@code true} if URI resolvers is enabled, {@code false} by default
     */
    @Override
    public boolean isEnabled() {
        return TRUE.equals(System.getProperty(PARQUET_TABLE_RESOLVER_ENABLED_KEY, FALSE));
    }

    @Override
    public boolean isEnabled(URI uri) {
        // TODO: format for allow/deny lists
        return true;
    }

    @Override
    public String helpEnable() {
        return String.format("To enable, set system property '%s' to 'true'.", PARQUET_TABLE_RESOLVER_ENABLED_KEY);
    }

    @Override
    public String helpEnable(URI uri) {
        throw new IllegalStateException();
    }
}

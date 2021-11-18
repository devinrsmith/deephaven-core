package io.deephaven.grpc_api.uri;

import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;

public final class ParquetTableResolverSimple extends ParquetTableResolver {
    public static final String PARQUET_TABLE_RESOLVER_ENABLED_KEY =
            UriRouterPropertyConfig.propertyKey(ParquetTableResolver.class);

    /**
     * The {@code true} value.
     */
    public static final String TRUE = "true";
    /**
     * The {@code false} value.
     */
    public static final String FALSE = "false";

    @Inject
    public ParquetTableResolverSimple() {

    }

    /**
     * {@code true} if {@link AuthContext#isSuperUser()}, otherwise looks up the property key
     * {@link #PARQUET_TABLE_RESOLVER_ENABLED_KEY}, {@code true} when equal to {@value #TRUE}; {@code false} otherwise.
     *
     * @return {@code true} if URI resolvers is enabled, {@code false} by default
     */
    @Override
    public boolean isEnabled(AuthContext auth) {
        return (auth != null && auth.isSuperUser()) || TRUE.equals(System.getProperty(PARQUET_TABLE_RESOLVER_ENABLED_KEY, FALSE));
    }

    @Override
    public boolean isEnabled(AuthContext auth, String item) {
        return true;
    }

    @Override
    public String helpEnable(AuthContext auth) {
        return String.format("Enabled for super-users. To enable for all, set system property '%s' to 'true'.",
                PARQUET_TABLE_RESOLVER_ENABLED_KEY);
    }

    @Override
    public String helpEnable(AuthContext auth, String item) {
        throw new IllegalStateException();
    }
}

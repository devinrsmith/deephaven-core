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

    @Override
    public Authorization<String> authorization(AuthorizationScope<String> scope, AuthContext context) {
        if (scope.isWrite()) {
            return Authorization.deny(scope, "The Parquet resolver does not allow publishing");
        }
        if (context != null && context.isSuperUser()) {
            return Authorization.allow(scope);
        }
        if (TRUE.equals(System.getProperty(PARQUET_TABLE_RESOLVER_ENABLED_KEY, FALSE))) {
            return Authorization.allow(scope);
        }
        return Authorization.deny(scope, String.format(
                "The Parquet resolver is enabled for super-users. To enable for all, set system property '%s' to 'true'.",
                PARQUET_TABLE_RESOLVER_ENABLED_KEY));
    }
}

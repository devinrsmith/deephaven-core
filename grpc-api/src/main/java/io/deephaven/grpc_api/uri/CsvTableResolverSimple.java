package io.deephaven.grpc_api.uri;

import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;

public final class CsvTableResolverSimple extends CsvTableResolver {

    public static final String CSV_TABLE_RESOLVER_ENABLED_KEY =
            UriRouterPropertyConfig.propertyKey(CsvTableResolver.class);

    /**
     * The {@code true} value.
     */
    public static final String TRUE = "true";

    /**
     * The {@code false} value.
     */
    public static final String FALSE = "false";

    @Inject
    public CsvTableResolverSimple() {}

    @Override
    public Authorization<String> authorization(AuthorizationScope<String> scope, AuthContext context) {
        if (scope.isWrite()) {
            return Authorization.deny(scope, "The CSV resolver does not allow publishing");
        }
        if (context != null && context.isSuperUser()) {
            return Authorization.allow(scope);
        }
        if (TRUE.equals(System.getProperty(CSV_TABLE_RESOLVER_ENABLED_KEY, FALSE))) {
            return Authorization.allow(scope);
        }
        return Authorization.deny(scope, String.format(
                "The CSV resolver is enabled for super-users. To enable for all, set system property '%s' to 'true'.",
                CSV_TABLE_RESOLVER_ENABLED_KEY));
    }
}

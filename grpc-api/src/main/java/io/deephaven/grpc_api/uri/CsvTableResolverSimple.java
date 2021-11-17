package io.deephaven.grpc_api.uri;

import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;
import java.net.URI;

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

    /**
     * {@code true} if {@link AuthContext#isSuperUser()}, otherwise looks up the property key
     * {@link #CSV_TABLE_RESOLVER_ENABLED_KEY}, {@code true} when equal to {@value #TRUE}; {@code false} otherwise.
     *
     * @return {@code true} if URI resolvers is enabled, {@code false} by default
     */
    @Override
    public boolean isEnabled(AuthContext auth) {
        return auth.isSuperUser() || TRUE.equals(System.getProperty(CSV_TABLE_RESOLVER_ENABLED_KEY, FALSE));
    }

    @Override
    public boolean isEnabled(AuthContext auth, String item) {
        return true;
    }

    @Override
    public String helpEnable(AuthContext auth) {
        return String.format("Enabled for super-users. To enable for all, set system property '%s' to 'true'.",
                CSV_TABLE_RESOLVER_ENABLED_KEY);
    }

    @Override
    public String helpEnable(AuthContext auth, String item) {
        throw new IllegalStateException();
    }
}

package io.deephaven.grpc_api.uri;

import javax.inject.Inject;
import java.net.URI;

public class CsvTableResolverPropertyConfig implements CsvTableResolver.Config {

    public static final String CSV_TABLE_RESOLVER_ENABLED_KEY = "deephaven.resolver.CsvTableResolver.enabled";

    /**
     * The {@code true} value.
     */
    public static final String TRUE = "true";

    /**
     * The {@code false} value.
     */
    public static final String FALSE = "false";

    @Inject
    public CsvTableResolverPropertyConfig() {
        final String expected = UriResolversPropertyConfig.propertyKey(CsvTableResolver.class);
        if (!CSV_TABLE_RESOLVER_ENABLED_KEY.equals(expected)) {
            throw new IllegalStateException(String.format("The GLOBAL_KEY constant '%s' should be updated to '%s'",
                    CSV_TABLE_RESOLVER_ENABLED_KEY, expected));
        }
    }

    /**
     * Looks up the property key {@value CSV_TABLE_RESOLVER_ENABLED_KEY}, {@code true} when equal to {@value TRUE};
     * {@code false} otherwise.
     *
     * @return {@code true} if URI resolvers is enabled, {@code false} by default
     */
    @Override
    public boolean isEnabled() {
        return TRUE.equals(System.getProperty(CSV_TABLE_RESOLVER_ENABLED_KEY, FALSE));
    }

    @Override
    public boolean isEnabled(URI uri) {
        // TODO: format for allow/deny lists
        return true;
    }

    @Override
    public String helpEnable() {
        return String.format("To enable, set system property '%s' to 'true'.", CSV_TABLE_RESOLVER_ENABLED_KEY);
    }

    @Override
    public String helpEnable(URI uri) {
        throw new IllegalStateException();
    }
}

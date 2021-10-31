package io.deephaven.grpc_api.uri;

import io.deephaven.uri.RemoteUri;

import javax.inject.Inject;

public class BarrageTableResolverPropertyConfig implements BarrageTableResolver.Config {

    public static final String BARRAGE_TABLE_RESOLVER_ENABLED_KEY = "deephaven.resolver.BarrageTableResolver.enabled";

    /**
     * The {@code true} value.
     */
    public static final String TRUE = "true";

    /**
     * The {@code false} value.
     */
    public static final String FALSE = "false";

    @Inject
    public BarrageTableResolverPropertyConfig() {
        final String expected = UriResolversPropertyConfig.propertyKey(BarrageTableResolver.class);
        if (!BARRAGE_TABLE_RESOLVER_ENABLED_KEY.equals(expected)) {
            throw new IllegalStateException(String.format("The GLOBAL_KEY constant '%s' should be updated to '%s'",
                    BARRAGE_TABLE_RESOLVER_ENABLED_KEY, expected));
        }
    }

    /**
     * Looks up the property key {@value BARRAGE_TABLE_RESOLVER_ENABLED_KEY}, {@code true} when equal to {@value TRUE};
     * {@code false} otherwise.
     *
     * @return {@code true} if URI resolvers is enabled, {@code false} by default
     */
    @Override
    public boolean isEnabled() {
        return TRUE.equals(System.getProperty(BARRAGE_TABLE_RESOLVER_ENABLED_KEY, FALSE));
    }

    @Override
    public boolean isEnabled(RemoteUri uri) {
        // TODO: format for allow/deny lists
        return true;
    }

    @Override
    public String helpEnable() {
        return String.format("To enable, set system property '%s' to 'true'.", BARRAGE_TABLE_RESOLVER_ENABLED_KEY);
    }

    @Override
    public String helpEnable(RemoteUri uri) {
        throw new IllegalStateException();
    }
}

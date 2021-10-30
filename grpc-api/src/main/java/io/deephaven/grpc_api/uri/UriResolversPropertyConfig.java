package io.deephaven.grpc_api.uri;

import io.deephaven.grpc_api.console.ConsoleServiceGrpcImpl;
import io.deephaven.grpc_api.uri.UriResolvers.Config;

import javax.inject.Inject;

import static io.deephaven.grpc_api.console.ConsoleServiceGrpcImpl.REMOTE_CONSOLE_DISABLED;

/**
 *
 */
public final class UriResolversPropertyConfig implements Config {

    /**
     * The global property key.
     */
    public static final String GLOBAL_ENABLED_KEY = "deephaven.resolvers.enabled";

    /**
     * The individual property key format. Must apply the class name with {@link String#format(String, Object...)}.
     */
    public static final String SPECIFIC_KEY_FORMAT = "deephaven.resolver.%s.enabled";

    /**
     * The true value.
     */
    public static final String TRUE = "true";

    /**
     * The prefix.
     */
    public static final String SIMPLIFY_PREFIX = "io.deephaven.grpc_api.uri.";

    @Inject
    public UriResolversPropertyConfig() {}

    /**
     * Looks up the property key {@value GLOBAL_ENABLED_KEY}, {@code true} when equal to {@value TRUE} or the property
     * is not set.
     *
     * @return {@code true} if URI resolvers is enabled, {@code true} by default
     */
    @Override
    public boolean isEnabled() {
        return TRUE.equals(System.getProperty(GLOBAL_ENABLED_KEY, TRUE));
    }

    /**
     * Looks up the property key format {@value SPECIFIC_KEY_FORMAT}, applied against the simplified class name for
     * {@code resolver}. The simplified class name is the class name with the prefix {@value SIMPLIFY_PREFIX} stripped
     * if present. If the property value is explicitly set to {@value TRUE}, then return {@code true}; if explicitly set
     * to anything else, then return {@code false}.
     *
     * <p>
     * Otherwise, returns the value of {@link UriResolver#isSafe()} from {@code resolver}.
     *
     * @param resolver the resolver
     * @return {@code true} if enabled
     */
    @Override
    public boolean isEnabled(UriResolver resolver) {
        final String propertyKey = String.format(SPECIFIC_KEY_FORMAT, simplifyName(resolver.getClass()));
        final String enabled = System.getProperty(propertyKey, null);
        if (TRUE.equals(enabled)) {
            // If explicitly set to "true"
            return true;
        } else if (enabled != null) {
            // If explicitly set to anything else
            return false;
        }
        // Otherwise, fall back on resolver default
        //return resolver.isSafe(); // todo get rid of this
    }

    @Override
    public String helpEnable() {
        return String.format("To enable, set system property '%s' to 'true' (or, remove 'false' entry)",
                GLOBAL_ENABLED_KEY);
    }

    @Override
    public String helpEnable(UriResolver resolver) {
        final String propertyKey = String.format(SPECIFIC_KEY_FORMAT, simplifyName(resolver.getClass()));
        return String.format("To enable, set system property '%s' to 'true'.", propertyKey);
    }

    private static String simplifyName(Class<? extends UriResolver> clazz) {
        final String name = clazz.getName();
        if (name.startsWith(SIMPLIFY_PREFIX)) {
            return name.substring(SIMPLIFY_PREFIX.length());
        }
        return name;
    }
}

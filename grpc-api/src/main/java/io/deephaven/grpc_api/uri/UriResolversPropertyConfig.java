package io.deephaven.grpc_api.uri;

import io.deephaven.grpc_api.uri.UriResolvers.Config;

import javax.inject.Inject;

/**
 * An enabled-by-default system property based configuration layer for {@link UriResolvers}.
 *
 * <p>
 * All resolvers can be disabled by setting the system property {@value GLOBAL_ENABLED_KEY} to {@value FALSE}; see
 * {@link #isEnabled()}.
 *
 * <p>
 * A specific resolver can be disabled by setting the system property {@value SPECIFIC_KEY_FORMAT} to {@value FALSE}
 * with the appropriate class name; see {@link #isEnabled(UriResolver)}.
 *
 * <p>
 * A {@link UriResolver} which has security concerns should have its own configuration layer with a disabled-by-default
 * attitude.
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
     * The {@code true} value.
     */
    public static final String TRUE = "true";

    /**
     * The {@code false} value.
     */
    public static final String FALSE = "false";

    /**
     * The prefix to simplify class names.
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
     * if present. Returns {@code true} when the value is equal to {@value TRUE} or the property is not set.
     *
     * @param resolver the resolver
     * @return {@code true} if enabled
     */
    @Override
    public boolean isEnabled(UriResolver resolver) {
        return TRUE.equals(System.getProperty(propertyKey(resolver.getClass()), TRUE));
    }

    @Override
    public String helpEnable() {
        return String.format("To enable, set system property '%s' to 'true' (or, remove the 'false' entry).",
                GLOBAL_ENABLED_KEY);
    }

    @Override
    public String helpEnable(UriResolver resolver) {
        final String propertyKey = String.format(SPECIFIC_KEY_FORMAT, simplifyName(resolver.getClass()));
        return String.format("To enable, set system property '%s' to 'true' (or, remove the 'false' entry).",
                propertyKey);
    }


    public static String propertyKey(Class<? extends UriResolver> clazz) {
        return String.format(SPECIFIC_KEY_FORMAT, simplifyName(clazz));
    }

    private static String simplifyName(Class<? extends UriResolver> clazz) {
        final String name = clazz.getName();
        if (name.startsWith(SIMPLIFY_PREFIX)) {
            return name.substring(SIMPLIFY_PREFIX.length());
        }
        return name;
    }
}

package io.deephaven.grpc_api.uri;

import javax.inject.Inject;

import static io.deephaven.grpc_api.console.ConsoleServiceGrpcImpl.REMOTE_CONSOLE_DISABLED;

public final class UriResolversPropertyConfig implements UriResolversConfig {

    public static final String GLOBAL_ENABLED_KEY = "deephaven.resolvers.enabled";

    public static final String SPECIFIC_KEY_FORMAT = "deephaven.resolver.%s.enabled";

    @Inject
    public UriResolversPropertyConfig() {
    }

    @Override
    public boolean isEnabled() {
        return "true".equals(System.getProperty(GLOBAL_ENABLED_KEY, "true"));
    }

    @Override
    public boolean isEnabled(UriResolver resolver) {
        final String propertyKey = String.format(SPECIFIC_KEY_FORMAT, simplifyName(resolver.getClass()));
        final String enabled = System.getProperty(propertyKey, null);
        if ("true".equals(enabled)) {
            // If explicitly set to "true"
            return true;
        } else if (enabled != null) {
            // If explicitly set to anything else
            return false;
        }
        // No explicit property set
        if (!REMOTE_CONSOLE_DISABLED) {
            // When the remote console is enabled, no additional security concerns
            return true;
        }
        // Otherwise, fall back on resolver default
        return resolver.isSafe();
    }

    @Override
    public String helpEnable() {
        return String.format("To enable, set system property '%s' to 'true' (or, remove 'false' entry)", GLOBAL_ENABLED_KEY);
    }

    @Override
    public String helpEnable(UriResolver resolver) {
        final String propertyKey = String.format(SPECIFIC_KEY_FORMAT, simplifyName(resolver.getClass()));
        return String.format("To enable, set system property '%s' to 'true'.", propertyKey);
    }

    private static String simplifyName(Class<? extends UriResolver> clazz) {
        final String name = clazz.getName();
        final String dhClassPrefix = "io.deephaven.grpc_api.uri.";
        if (name.startsWith(dhClassPrefix)) {
            return name.substring(dhClassPrefix.length());
        }
        return name;
    }
}

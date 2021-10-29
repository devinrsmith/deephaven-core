package io.deephaven.grpc_api.uri;

public interface UriResolversConfig {
    boolean isEnabled();

    boolean isEnabled(Class<? extends UriResolver> clazz);

    String helpEnable();

    String helpEnable(Class<? extends UriResolver> clazz);
}

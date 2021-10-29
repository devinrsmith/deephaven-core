package io.deephaven.grpc_api.uri;

public interface UriResolversConfig {
    boolean isEnabled();

    boolean isEnabled(UriResolver resolver);

    String helpEnable();

    String helpEnable(UriResolver resolver);
}

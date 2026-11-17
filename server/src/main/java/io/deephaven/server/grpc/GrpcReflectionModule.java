//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.grpc;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.util.AuthorizationWrappedGrpcBinding;
import io.grpc.BindableService;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.reflection.v1.ServerReflectionGrpc;

import java.util.Collections;
import java.util.Set;

/**
 * Binds the gRPC server reflection service ({@code grpc.reflection.v1.ServerReflection}) when the configuration
 * property {@value #REFLECTION_ENABLED_PROP} is {@code true}. It is disabled by default.
 *
 * <p>
 * Reflection exposes the schema of every service on the server, so access is decided by the
 * {@link AuthorizationProvider}'s {@link AuthorizationProvider#getServerReflectionAuthWiring() ServerReflection
 * wiring}.
 */
@Module
public class GrpcReflectionModule {

    /**
     * Set to {@code true} to enable the gRPC server reflection service. Defaults to {@code false}.
     */
    public static final String REFLECTION_ENABLED_PROP = "grpc.reflection.enabled";

    @Provides
    @ElementsIntoSet
    static Set<BindableService> reflectionService(final AuthorizationProvider authorizationProvider) {
        if (!Configuration.getInstance().getBooleanWithDefault(REFLECTION_ENABLED_PROP, false)) {
            return Collections.emptySet();
        }
        return Set.of(new AuthorizationWrappedGrpcBinding<>(
                authorizationProvider.getServerReflectionAuthWiring(),
                (ServerReflectionGrpc.ServerReflectionImplBase) ProtoReflectionServiceV1.newInstance()));
    }
}

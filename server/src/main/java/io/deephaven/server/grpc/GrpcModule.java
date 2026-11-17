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
import io.grpc.ServerInterceptor;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.reflection.v1.ServerReflectionGrpc;

import java.util.Collections;
import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * Binds the gRPC services that are decided at runtime, rather than at compile time:
 * <ul>
 * <li>Any {@link BindableService} registered with the {@link ServiceLoader} (a
 * {@code META-INF/services/io.grpc.BindableService} entry on the classpath). These are bound as-is; they are not
 * wrapped in any authorization, so an implementation is responsible for its own.</li>
 * <li>The gRPC server reflection service ({@code grpc.reflection.v1.ServerReflection}), when the configuration property
 * {@value #REFLECTION_ENABLED_PROP} is {@code true}. It is disabled by default. Reflection exposes the schema of every
 * service on the server, so access is decided by the {@link AuthorizationProvider}'s
 * {@link AuthorizationProvider#getServerReflectionAuthWiring() ServerReflection wiring}.</li>
 * </ul>
 */
@Module
public class GrpcModule {

    /**
     * Set to {@code true} to enable the gRPC server reflection service. Defaults to {@code false}.
     */
    public static final String REFLECTION_ENABLED_PROP = "grpc.reflection.enabled";

    @Provides
    @ElementsIntoSet
    static Set<BindableService> primeServices() {
        return Collections.emptySet();
    }

    @Provides
    @ElementsIntoSet
    static Set<BindableService> serviceLoaderServices() {
        final Set<BindableService> services = new HashSet<>();
        ServiceLoader.load(BindableService.class).forEach(services::add);
        return services;
    }

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

    @Provides
    @ElementsIntoSet
    static Set<ServerInterceptor> primeInterceptors() {
        return Collections.emptySet();
    }
}

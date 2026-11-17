//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import io.deephaven.auth.codegen.impl.ServerReflectionAuthWiring;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.test.TestAuthorizationProvider;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.reflection.v1.ServerReflectionGrpc;
import io.grpc.reflection.v1.ServerReflectionRequest;
import io.grpc.reflection.v1.ServerReflectionResponse;
import io.grpc.reflection.v1.ServiceResponse;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * Tests that {@code grpc.reflection.enabled} turns the gRPC reflection service on, and that the authorization wiring
 * governs access.
 */
public class GrpcReflectionTest extends DeephavenApiServerSingleAuthenticatedBase {
    private static final String PROP = "grpc.reflection.enabled";
    private final TestAuthorizationProvider authProvider = new TestAuthorizationProvider();

    @Override
    protected AuthorizationProvider authorizationProvider() {
        return authProvider;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        System.setProperty(PROP, "true");
        super.setUp();
    }

    @After
    public void clearProperty() {
        System.clearProperty(PROP);
    }

    @Test
    public void listsServicesWhenAuthenticated() throws Exception {
        final Set<String> names = listServices(channel.channel());
        assertThat(names).contains("grpc.reflection.v1.ServerReflection",
                "io.deephaven.proto.backplane.grpc.SessionService");
    }

    @Test
    public void deniedByAuthorizationWiring() throws Exception {
        authProvider.getServerReflectionAuthWiring().delegate = new ServerReflectionAuthWiring.DenyAll();
        try {
            listServices(channel.channel());
            failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
        } catch (java.util.concurrent.ExecutionException e) {
            assertThat(Status.fromThrowable(e.getCause()).getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
        }
    }

    private static Set<String> listServices(final Channel channel) throws Exception {
        final CompletableFuture<ServerReflectionResponse> result = new CompletableFuture<>();
        final StreamObserver<ServerReflectionRequest> requests = ServerReflectionGrpc.newStub(channel)
                .serverReflectionInfo(new StreamObserver<>() {
                    @Override
                    public void onNext(final ServerReflectionResponse value) {
                        result.complete(value);
                    }

                    @Override
                    public void onError(final Throwable t) {
                        result.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        result.completeExceptionally(new IllegalStateException("completed without a response"));
                    }
                });
        requests.onNext(ServerReflectionRequest.newBuilder().setListServices("").build());
        requests.onCompleted();
        return result.get(30, TimeUnit.SECONDS).getListServicesResponse().getServiceList().stream()
                .map(ServiceResponse::getName).collect(Collectors.toSet());
    }
}

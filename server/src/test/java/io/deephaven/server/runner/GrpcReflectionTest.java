//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import io.deephaven.auth.codegen.impl.ServerReflectionAuthWiring;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.grpc.GrpcReflectionModule;
import io.deephaven.server.test.TestAuthorizationProvider;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.reflection.v1.ServerReflectionGrpc;
import io.grpc.reflection.v1.ServerReflectionRequest;
import io.grpc.reflection.v1.ServerReflectionResponse;
import io.grpc.reflection.v1.ServiceResponse;
import io.grpc.stub.BlockingClientCall;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * Tests that {@code grpc.reflection.enabled} turns the gRPC reflection service on, and that the authorization wiring
 * governs access.
 */
public class GrpcReflectionTest extends DeephavenApiServerSingleAuthenticatedBase {
    private final TestAuthorizationProvider authProvider = new TestAuthorizationProvider();

    @Override
    protected AuthorizationProvider authorizationProvider() {
        return authProvider;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        System.setProperty(GrpcReflectionModule.REFLECTION_ENABLED_PROP, "true");
        super.setUp();
    }

    @After
    public void clearProperty() {
        System.clearProperty(GrpcReflectionModule.REFLECTION_ENABLED_PROP);
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
            failBecauseExceptionWasNotThrown(StatusException.class);
        } catch (StatusException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
        }
    }

    private static Set<String> listServices(final Channel channel) throws Exception {
        final BlockingClientCall<ServerReflectionRequest, ServerReflectionResponse> call =
                ServerReflectionGrpc.newBlockingV2Stub(channel).serverReflectionInfo();
        try {
            call.write(ServerReflectionRequest.newBuilder().setListServices("").build());
            call.halfClose();
            final ServerReflectionResponse response = call.read(30, TimeUnit.SECONDS);
            return response.getListServicesResponse().getServiceList().stream()
                    .map(ServiceResponse::getName).collect(Collectors.toSet());
        } finally {
            call.cancel("done", null);
        }
    }
}

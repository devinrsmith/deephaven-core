//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import io.deephaven.auth.codegen.impl.ServerReflectionAuthWiring;
import io.deephaven.configuration.Configuration;
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
    private static final long TIMEOUT_SECONDS = 30;
    private final TestAuthorizationProvider authProvider = new TestAuthorizationProvider();
    private String previousEnabled;

    @Override
    protected AuthorizationProvider authorizationProvider() {
        return authProvider;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        previousEnabled = Configuration.getInstance().setProperty(GrpcReflectionModule.REFLECTION_ENABLED_PROP, "true");
        super.setUp();
    }

    @After
    public void clearProperty() {
        Configuration.getInstance().setProperty(GrpcReflectionModule.REFLECTION_ENABLED_PROP, previousEnabled);
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
        final ServerReflectionResponse response;
        try {
            // Throws TimeoutException on timeout. The return value is deliberately ignored: if the call was already
            // closed (e.g. the request was denied), the read below reports the call's status.
            call.write(ServerReflectionRequest.newBuilder().setListServices("").build(), TIMEOUT_SECONDS,
                    TimeUnit.SECONDS);
            call.halfClose();
            response = call.read(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            call.cancel("done", null);
        }
        assertThat(response).as("reflection response").isNotNull();
        return response.getListServicesResponse().getServiceList().stream()
                .map(ServiceResponse::getName).collect(Collectors.toSet());
    }
}

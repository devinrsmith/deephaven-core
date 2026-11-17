//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.auth.codegen.impl;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.ServiceAuthWiring;
import io.grpc.ServerServiceDefinition;
import io.grpc.reflection.v1.ServerReflectionGrpc;
import io.grpc.reflection.v1.ServerReflectionRequest;

/**
 * This interface provides type-safe authorization hooks for ServerReflectionGrpc.
 */
public interface ServerReflectionAuthWiring extends ServiceAuthWiring<ServerReflectionGrpc.ServerReflectionImplBase> {
    /**
     * Wrap the real implementation with authorization checks.
     *
     * @param delegate the real service implementation
     * @return the wrapped service implementation
     */
    default ServerServiceDefinition intercept(ServerReflectionGrpc.ServerReflectionImplBase delegate) {
        final ServerServiceDefinition service = delegate.bindService();
        final ServerServiceDefinition.Builder serviceBuilder =
                ServerServiceDefinition.builder(service.getServiceDescriptor());

        serviceBuilder.addMethod(ServiceAuthWiring.intercept(
                service, "ServerReflectionInfo", this::onCallStartedServerReflectionInfo,
                this::onMessageReceivedServerReflectionInfo));

        return serviceBuilder.build();
    }

    /**
     * Authorize a request to open a bidirectional-streaming rpc ServerReflectionInfo.
     *
     * @param authContext the authentication context of the request
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke ServerReflectionInfo
     */
    void onCallStartedServerReflectionInfo(AuthContext authContext);

    /**
     * Authorize a request to ServerReflectionInfo.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke ServerReflectionInfo
     */
    void onMessageReceivedServerReflectionInfo(AuthContext authContext, ServerReflectionRequest request);

    class AllowAll implements ServerReflectionAuthWiring {
        public void onCallStartedServerReflectionInfo(AuthContext authContext) {}

        public void onMessageReceivedServerReflectionInfo(AuthContext authContext, ServerReflectionRequest request) {}
    }

    class DenyAll implements ServerReflectionAuthWiring {
        public void onCallStartedServerReflectionInfo(AuthContext authContext) {
            ServiceAuthWiring.operationNotAllowed();
        }

        public void onMessageReceivedServerReflectionInfo(AuthContext authContext, ServerReflectionRequest request) {
            ServiceAuthWiring.operationNotAllowed();
        }
    }

    class TestUseOnly implements ServerReflectionAuthWiring {
        public ServerReflectionAuthWiring delegate;

        public void onCallStartedServerReflectionInfo(AuthContext authContext) {
            if (delegate != null) {
                delegate.onCallStartedServerReflectionInfo(authContext);
            }
        }

        public void onMessageReceivedServerReflectionInfo(AuthContext authContext, ServerReflectionRequest request) {
            if (delegate != null) {
                delegate.onMessageReceivedServerReflectionInfo(authContext, request);
            }
        }
    }
}

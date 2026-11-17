//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.grpc;

import io.deephaven.server.session.SessionService;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.Objects;

/**
 * Rejects calls that are not attached to a live session with {@link Status#UNAUTHENTICATED}. Must run inside the
 * session interceptor, which installs the session on the gRPC context; for services added to the server builder, that
 * is the case when this interceptor is applied directly to the service definition.
 */
final class RequireSessionInterceptor implements ServerInterceptor {
    private final SessionService sessionService;

    RequireSessionInterceptor(final SessionService sessionService) {
        this.sessionService = Objects.requireNonNull(sessionService);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            final ServerCall<ReqT, RespT> call, final Metadata headers, final ServerCallHandler<ReqT, RespT> next) {
        if (sessionService.getOptionalSession() == null) {
            call.close(Status.UNAUTHENTICATED, new Metadata());
            return new ServerCall.Listener<>() {};
        }
        return next.startCall(call, headers);
    }
}

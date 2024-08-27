//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.util.AuthorizationWrappedGrpcBinding;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

@Module
public interface SessionModule {
    @Provides
    @IntoSet
    static BindableService bindSessionServiceGrpcImpl(
            AuthorizationProvider authProvider, SessionServiceGrpcImpl sessionServiceGrpc) {
        return new AuthorizationWrappedGrpcBinding<>(
                authProvider.getSessionServiceAuthWiring(), sessionServiceGrpc);
    }

    @Binds
    @IntoSet
    ServerInterceptor bindSessionServiceInterceptor(
            SessionServiceGrpcImpl.SessionServiceInterceptor sessionServiceGrpcInterceptor);

    @Binds
    @IntoSet
    TicketResolver bindSessionTicketResolverServerSideExports(ExportTicketResolver resolver);

    @Binds
    @IntoSet
    TicketResolver bindSharedTicketResolver(SharedTicketResolver resolver);

    @Binds
    @IntoSet
    SessionListener bindsSessionListenerLogger(SessionListenerLogger listener);
}

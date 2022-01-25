package io.deephaven.server.appmode;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.plugin.app.App;
import io.deephaven.server.session.TicketResolver;
import io.grpc.BindableService;

import javax.inject.Singleton;

@Module
public interface AppModeModule {
    @Binds
    @IntoSet
    BindableService bindApplicationServiceImpl(ApplicationServiceGrpcImpl applicationService);

    @Binds
    @IntoSet
    TicketResolver bindApplicationTicketResolver(ApplicationTicketResolver resolver);

    @Binds
    ScriptSession.Listener bindScriptSessionListener(ApplicationServiceGrpcImpl applicationService);

    @Binds
    ApplicationState.Listener bindApplicationStateListener(ApplicationServiceGrpcImpl applicationService);

    @Binds
    ApplicationStates bindApplicationStates(Applications applications);

    @Provides
    @Singleton
    static Applications providesApplications() {
        return Applications.create();
    }

    @Binds
    @IntoSet
    App bindsApplication(ApplicationApplication plugin);
}

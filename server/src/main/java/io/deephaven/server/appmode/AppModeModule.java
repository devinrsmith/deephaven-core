package io.deephaven.server.appmode;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.plugin.app.App;
import io.deephaven.plugin.app.AppStates;
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

    @Provides
    static FieldState providesFields(ApplicationServiceGrpcImpl impl) {
        return impl.fields();
    }

    @Provides
    @IntoSet
    static App providesApplication(AppStateYo plugin) {
        return new AppStates("io.deephaven.Applications", "todo", plugin);
    }
}

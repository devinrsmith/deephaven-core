package io.deephaven.server.appmode;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import dagger.multibindings.IntoSet;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.server.appmode.Applications.Listener;
import io.deephaven.server.session.TicketResolver;
import io.grpc.BindableService;

import java.util.Collections;
import java.util.Set;

@Module(includes = {ApplicationApplicationModule.class})
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
    @IntoSet
    ApplicationState.Listener bindApplicationStateListener(ApplicationServiceGrpcImpl applicationService);

    @Binds
    ApplicationStates bindApplicationStates(Applications applications);

    @Provides
    @ElementsIntoSet
    static Set<Listener> primeApplicationsListener() {
        return Collections.emptySet();
    }
}

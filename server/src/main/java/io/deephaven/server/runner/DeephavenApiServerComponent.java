package io.deephaven.server.runner;

import dagger.BindsInstance;
import dagger.Module;
import io.deephaven.server.healthcheck.HealthCheckModule;
import io.deephaven.server.python.PythonModule;

import javax.inject.Named;
import java.io.PrintStream;

public interface DeephavenApiServerComponent {

    DeephavenApiServer getServer();

    @Module(includes = {
            DeephavenApiServerModule.class,
            HealthCheckModule.class,
            PythonModule.class
    })
    interface MainModule {
        interface Builder<B extends Builder<B>> {
            @BindsInstance
            B withPort(@Named("http.port") int port);

            @BindsInstance
            B withSchedulerPoolSize(@Named("scheduler.poolSize") int numThreads);

            @BindsInstance
            B withSessionTokenExpireTmMs(@Named("session.tokenExpireMs") long tokenExpireMs);

            @BindsInstance
            B withMaxInboundMessageSize(@Named("grpc.maxInboundMessageSize") int maxInboundMessageSize);

            @BindsInstance
            B withOut(@Named("out") PrintStream out);

            @BindsInstance
            B withErr(@Named("err") PrintStream err);
        }
    }

    @Module(includes = {
            DeephavenApiServerModule.class,
            ServerBuilderInProcessModule.class
    })
    interface InProcessModule {
        interface Builder<B extends Builder<B>> {

            @BindsInstance
            B withSchedulerPoolSize(@Named("scheduler.poolSize") int numThreads);

            @BindsInstance
            B withSessionTokenExpireTmMs(@Named("session.tokenExpireMs") long tokenExpireMs);

            @BindsInstance
            B withOut(@Named("out") PrintStream out);

            @BindsInstance
            B withErr(@Named("err") PrintStream err);
        }
    }
}

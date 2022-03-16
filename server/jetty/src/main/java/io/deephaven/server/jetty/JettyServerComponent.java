package io.deephaven.server.jetty;

import dagger.Component;
import io.deephaven.march.MarchComponent;
import io.deephaven.march.MarchMadnessModule;
import io.deephaven.server.healthcheck.HealthCheckModule;
import io.deephaven.server.plugin.python.PythonPluginsRegistration;
import io.deephaven.server.runner.DeephavenApiServerComponent;
import io.deephaven.server.runner.DeephavenApiServerModule;

import javax.inject.Singleton;

@Singleton
@Component(modules = {
        DeephavenApiServerModule.class,
        HealthCheckModule.class,
        PythonPluginsRegistration.Module.class,
        JettyServerModule.class,
        MarchMadnessModule.class,
})
public interface JettyServerComponent extends DeephavenApiServerComponent, MarchComponent {
    @Component.Builder
    interface Builder extends DeephavenApiServerComponent.Builder<Builder> {
        JettyServerComponent build();
    }
}

package io.deephaven.server.runner;

import dagger.Component;
import io.deephaven.server.runner.DeephavenApiServerComponent.InProcessModule;
import io.grpc.ManagedChannelBuilder;

import javax.inject.Singleton;

@Singleton
@Component(modules = { InProcessModule.class })
public interface InProcessServerComponent extends DeephavenApiServerComponent {

    ManagedChannelBuilder<?> channelBuilder();

    @Component.Builder
    interface Builder extends InProcessModule.Builder<Builder> {
        InProcessServerComponent build();
    }
}

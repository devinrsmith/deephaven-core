package io.deephaven.grpc_api.appmode;

import io.deephaven.client.impl.BarrageLocalResolverInstance;
import io.deephaven.client.impl.BarrageLocalTableResolver;

import javax.inject.Inject;
import java.util.Objects;

public class BarrageLocalResolverInjector {

    private final BarrageLocalTableResolver resolver;

    @Inject
    public BarrageLocalResolverInjector(BarrageLocalTableResolver resolver) {
        this.resolver = Objects.requireNonNull(resolver);
    }

    public void init() {
        BarrageLocalResolverInstance.init(resolver);
    }
}

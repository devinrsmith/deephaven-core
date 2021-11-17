package io.deephaven.grpc_api.uri;

import dagger.Module;
import io.deephaven.grpc_api.barrage.BarrageClientModule;

/**
 * Installs the modules necessary for URI resolution.
 *
 * @see BarrageClientModule
 * @see UriResolversModule
 * @see UriTicketResolverModule
 */
@Module(includes = {
        BarrageClientModule.class,
        UriResolversModule.class,
        UriTicketResolverModule.class
})
public interface UriModule {

}

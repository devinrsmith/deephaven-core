package io.deephaven.grpc_api.uri;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import dagger.multibindings.IntoSet;
import io.deephaven.grpc_api.uri.UriResolversModule.BaseResolvers;
import io.deephaven.grpc_api.uri.UriResolversModule.PropertyConfigModule;
import io.deephaven.grpc_api.uri.UriResolversModule.ServiceLoaderResolvers;
import io.deephaven.grpc_api.uri.UriRouter.Config;

import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * @see PropertyConfigModule
 * @see ServiceLoaderResolvers
 * @see BaseResolvers
 */
@Module(includes = {
        PropertyConfigModule.class,
        ServiceLoaderResolvers.class,
        BaseResolvers.class
})
public interface UriResolversModule {
    /**
     * Installs the {@link UriResolver URI resolvers} based on {@link ServiceLoader#load(Class)} for 3rd-party
     * extensions.
     */
    @Module
    interface ServiceLoaderResolvers {
        @Provides
        @ElementsIntoSet
        static Set<UriResolver> providesUriResolvers() {
            final Set<UriResolver> resolvers = new HashSet<>();
            ServiceLoader.load(UriResolver.class).forEach(resolvers::add);
            return resolvers;
        }
    }

    /**
     * Installs the base {@link UriResolver URI resolvers}. See each specific resolver for more information.
     *
     * @see QueryScopeResolverOpen
     * @see ApplicationResolverOpen
     * @see BarrageTableResolverSimple
     * @see CsvTableResolverSimple
     * @see ParquetTableResolverSimple
     */
    @Module
    interface BaseResolvers {
        @Binds
        @IntoSet
        UriResolver bindQueryScopeResolver(QueryScopeResolverOpen resolver);

        @Binds
        @IntoSet
        UriResolver bindApplicationResolver(ApplicationResolverOpen resolver);

        @Binds
        @IntoSet
        UriResolver bindsBarrageTableResolver(BarrageTableResolverList resolver);

        @Binds
        @IntoSet
        UriResolver bindCsvResolver(CsvTableResolverSimple resolver);

        @Binds
        @IntoSet
        UriResolver bindParquetResolver(ParquetTableResolverSimple resolver);
    }

    /**
     * Binds property-based configurations.
     *
     * @see UriRouterPropertyConfig
     */
    @Module
    interface PropertyConfigModule {
        @Binds
        Config bindConfig(UriRouterPropertyConfig config);
    }
}

package io.deephaven.grpc_api.uri;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import dagger.multibindings.IntoSet;
import io.deephaven.grpc_api.uri.UriRouter.Config;
import io.deephaven.grpc_api.uri.UriResolversModule.BaseResolvers;
import io.deephaven.grpc_api.uri.UriResolversModule.PropertyConfigModule;
import io.deephaven.grpc_api.uri.UriResolversModule.ServiceLoaderResolvers;

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
     * @see BarrageTableResolverSimple
     * @see QueryScopeResolverOpen
     * @see ApplicationResolverOpen
     * @see CsvTableResolver
     * @see ParquetTableResolver
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
        UriResolver bindsBarrageTableResolver(BarrageTableResolverSimple resolver);

        @Binds
        @IntoSet
        UriResolver bindCsvResolver(CsvTableResolver resolver);

        @Binds
        @IntoSet
        UriResolver bindParquetResolver(ParquetTableResolver resolver);
    }

    /**
     * Binds property-based configurations.
     *
     * @see UriRouterPropertyConfig
     * @see CsvTableResolverPropertyConfig
     * @see ParquetTableResolverPropertyConfig
     */
    @Module
    interface PropertyConfigModule {
        @Binds
        Config bindConfig(UriRouterPropertyConfig config);

        @Binds
        CsvTableResolver.Config bindCsvTableResolverConfig(CsvTableResolverPropertyConfig config);

        @Binds
        ParquetTableResolver.Config bindParquetTableResolverConfig(ParquetTableResolverPropertyConfig config);
    }
}

package io.deephaven.grpc_api.uri;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import dagger.multibindings.IntoSet;
import io.deephaven.grpc_api.uri.UriResolvers.Config;
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
     * @see BarrageTableResolver
     * @see QueryScopeResolver
     * @see ApplicationResolver
     * @see CsvTableResolver
     * @see ParquetTableResolver
     */
    @Module
    interface BaseResolvers {
        @Binds
        @IntoSet
        UriResolver bindQueryScopeResolver(QueryScopeResolver resolver);

        @Binds
        @IntoSet
        UriResolver bindApplicationResolver(ApplicationResolver resolver);

        @Binds
        @IntoSet
        UriResolver bindsBarrageTableResolver(BarrageTableResolver resolver);

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
     * @see UriResolversPropertyConfig
     * @see BarrageTableResolverPropertyConfig
     * @see CsvTableResolverPropertyConfig
     * @see ParquetTableResolverPropertyConfig
     */
    @Module
    interface PropertyConfigModule {
        @Binds
        Config bindConfig(UriResolversPropertyConfig config);

        @Binds
        BarrageTableResolver.Config bindBarrageTableResolverConfig(BarrageTableResolverPropertyConfig config);

        @Binds
        CsvTableResolver.Config bindCsvTableResolverConfig(CsvTableResolverPropertyConfig config);

        @Binds
        ParquetTableResolver.Config bindParquetTableResolverConfig(ParquetTableResolverPropertyConfig config);
    }
}

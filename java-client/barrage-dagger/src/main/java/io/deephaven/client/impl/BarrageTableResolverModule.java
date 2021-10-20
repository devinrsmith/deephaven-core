/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.client.impl.BarrageSubcomponent.Builder;
import io.deephaven.uri.TableResolver;

@Module
public interface BarrageTableResolverModule {
    @Binds
    @IntoSet
    TableResolver bindsBarrageTableResolver(BarrageTableResolver resolver);

    @Provides
    static Builder providesFactoryBuilder() {
        return DaggerDeephavenBarrageRoot.create().factoryBuilder();
    }

    @Binds
    BarrageSessionFactoryBuilder bindsFactoryBuilder(Builder builder);
}

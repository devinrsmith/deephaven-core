package io.deephaven.grpc_api.barrage;

import dagger.Module;
import dagger.Provides;
import io.deephaven.client.impl.BarrageTableResolverModule;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import javax.inject.Singleton;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Module(includes = BarrageTableResolverModule.class)
public interface BarrageModule {

    @Provides
    @Singleton
    static BufferAllocator providesAllocator() {
        return new RootAllocator();
    }


    @Provides
    @Singleton
    static ScheduledExecutorService providesScheduler() {
        return Executors.newScheduledThreadPool(4);
    }
}

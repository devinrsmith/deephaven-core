package io.deephaven.grpc_api.csv;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.client.impl.BarrageTableResolverModule;
import io.deephaven.db.tables.utils.CsvTableResolver;
import io.deephaven.uri.TableResolver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import javax.inject.Singleton;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Module
public interface CsvModule {

    @Binds
    @IntoSet
    TableResolver bindCsvResolver(CsvTableResolver csvTableResolver);
}

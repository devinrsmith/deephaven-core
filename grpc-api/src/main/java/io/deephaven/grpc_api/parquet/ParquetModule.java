package io.deephaven.grpc_api.parquet;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.deephaven.db.tables.utils.CsvTableResolver;
import io.deephaven.db.tables.utils.ParquetTableResolver;
import io.deephaven.uri.TableResolver;

@Module
public interface ParquetModule {

    @Binds
    @IntoSet
    TableResolver bindCsvResolver(ParquetTableResolver parquetTableResolver);
}

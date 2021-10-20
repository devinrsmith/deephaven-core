package io.deephaven.db.tables.utils;

import io.deephaven.db.tables.Table;
import io.deephaven.uri.ResolvableUri;
import io.deephaven.uri.TableResolver;

import javax.inject.Inject;

public final class ParquetTableResolver implements TableResolver {

    @Inject
    public ParquetTableResolver() {}

    @Override
    public boolean canResolve(ResolvableUri uri) {
        return "parquet".equals(uri.scheme());
    }

    @Override
    public Table resolve(ResolvableUri uri) throws InterruptedException {
        if (!canResolve(uri)) {
            throw new IllegalArgumentException(String.format("Unable to resolve uri '%s'", uri));
        }
        return ParquetTools.readTable(uri.toUri().getPath());
    }
}

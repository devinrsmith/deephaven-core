package io.deephaven.uri;

import io.deephaven.db.tables.Table;

public interface TableResolver {

    boolean canResolve(DeephavenUri uri);

    Table resolve(DeephavenUri uri) throws InterruptedException;
}

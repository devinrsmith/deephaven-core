package io.deephaven.uri;

import io.deephaven.db.tables.Table;

public interface TableResolver {

    boolean canResolve(ResolvableUri uri);

    Table resolve(ResolvableUri uri) throws InterruptedException;
}

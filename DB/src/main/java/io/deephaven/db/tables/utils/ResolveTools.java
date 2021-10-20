package io.deephaven.db.tables.utils;

import io.deephaven.db.tables.Table;
import io.deephaven.uri.ResolvableUri;
import io.deephaven.uri.TableResolverInstance;

import java.net.URI;

public class ResolveTools {
    public static Table resolve(String uri) throws InterruptedException {
        return resolve(ResolvableUri.of(URI.create(uri)));
    }

    public static Table resolve(ResolvableUri uri) throws InterruptedException {
        return TableResolverInstance.get().resolve(uri);
    }
}

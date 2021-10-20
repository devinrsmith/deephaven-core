package io.deephaven.uri;

import io.deephaven.db.tables.Table;

import javax.inject.Inject;
import java.util.Objects;
import java.util.Set;

public final class TableResolverSet implements TableResolver {
    private final Set<TableResolver> resolvers;

    @Inject
    public TableResolverSet(Set<TableResolver> resolvers) {
        this.resolvers = Objects.requireNonNull(resolvers);
    }

    @Override
    public boolean canResolve(DeephavenUri uri) {
        for (TableResolver resolver : resolvers) {
            if (resolver.canResolve(uri)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Table resolve(DeephavenUri uri) throws InterruptedException {
        for (TableResolver resolver : resolvers) {
            if (resolver.canResolve(uri)) {
                return resolver.resolve(uri);
            }
        }
        throw new UnsupportedOperationException(String.format("Unable to find table resolver for '%s'", uri));
    }
}

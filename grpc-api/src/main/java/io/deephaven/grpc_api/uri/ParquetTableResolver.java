package io.deephaven.grpc_api.uri;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.ParquetTools;
import io.deephaven.uri.UriHelper;

import java.net.URI;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * The parquet table resolver is able to resolve local parquet files, or directories for the scheme {@value #SCHEME},
 * into {@link Table tables}.
 *
 * <p>
 * For example, {@code parquet:///data/my-file.parquet} or {@code parquet:///data/my-dir}.
 *
 * <p>
 * For more advanced use cases, see {@link ParquetTools}.
 */
public abstract class ParquetTableResolver extends UriResolverBase<String> {

    /**
     * The parquet scheme, {@code parquet}.
     */
    public static final String SCHEME = "parquet";

    public static boolean isWellFormed(URI uri) {
        return SCHEME.equals(uri.getScheme()) && UriHelper.isLocalPath(uri);
    }

    public static ParquetTableResolver get() {
        return UriRouterInstance.get().find(ParquetTableResolver.class).get();
    }

    @Override
    public final Set<String> schemes() {
        return Collections.singleton(SCHEME);
    }

    @Override
    public final boolean isResolvable(URI uri) {
        return isWellFormed(uri);
    }

    @Override
    public final String adaptToPath(URI uri) {
        return uri.getPath();
    }

    @Override
    public final URI adaptToUri(String item) {
        return URI.create(String.format("parquet://%s", item));
    }

    @Override
    public final Object resolveItem(String item) {
        return ParquetTools.readTable(item).coalesce(); // todo otherwise "is not a subscribable table"
    }

    @Override
    public void forAllPaths(BiConsumer<String, Object> consumer) {

    }
}

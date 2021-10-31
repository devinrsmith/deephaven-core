package io.deephaven.grpc_api.uri;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.ParquetTools;
import io.deephaven.uri.UriHelper;

import javax.inject.Inject;
import java.net.URI;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

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
public final class ParquetTableResolver implements UriResolver {

    /**
     * The parquet scheme, {@code parquet}.
     */
    public static final String SCHEME = "parquet";

    private static final Set<String> SCHEMES = Collections.singleton(SCHEME);

    public static boolean isWellFormed(URI uri) {
        return SCHEME.equals(uri.getScheme()) && UriHelper.isLocalPath(uri);
    }

    public static ParquetTableResolver get() {
        return UriResolversInstance.get().find(ParquetTableResolver.class).get();
    }

    private final Config config;

    @Inject
    public ParquetTableResolver(Config config) {
        this.config = Objects.requireNonNull(config);
    }

    @Override
    public Set<String> schemes() {
        return SCHEMES;
    }

    @Override
    public boolean isResolvable(URI uri) {
        return isWellFormed(uri);
    }

    @Override
    public Table resolve(URI uri) {
        if (!isWellFormed(uri)) {
            throw new IllegalArgumentException(String.format("Invalid parquet URI '%s'", uri));
        }
        return ParquetTools.readTable(uri.getPath());
    }

    @Override
    public Object resolveSafely(URI uri) {
        if (!config.isEnabled()) {
            throw new UnsupportedOperationException(
                    String.format("Parquet table resolver is disabled. %s", config.helpEnable()));
        }
        if (!config.isEnabled(uri)) {
            throw new UnsupportedOperationException(
                    String.format("Parquet table resolver is disable for URI '%s'. %s", uri, config.helpEnable(uri)));
        }
        return ParquetTools.readTable(uri.getPath());
    }

    public interface Config {

        boolean isEnabled();

        boolean isEnabled(URI uri);

        String helpEnable();

        String helpEnable(URI uri);
    }
}

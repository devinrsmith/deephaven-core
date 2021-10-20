package io.deephaven.db.tables.utils;

import io.deephaven.db.tables.Table;
import io.deephaven.uri.ResolvableUri;
import io.deephaven.uri.TableResolver;

import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;

public final class CsvTableResolver implements TableResolver {

    @Inject
    public CsvTableResolver() {}

    @Override
    public boolean canResolve(ResolvableUri uri) {
        switch (uri.scheme()) {
            case "csv+http":
            case "http+csv":
            case "csv+https":
            case "https+csv":
            case "csv+file":
            case "file+csv":
            case "csv":
                return true;
        }
        return false;
    }

    @Override
    public Table resolve(ResolvableUri uri) throws InterruptedException {
        try {
            return CsvHelpers.readCsv(csvString(uri));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String csvString(ResolvableUri uri) {
        switch (uri.scheme()) {
            case "csv+http":
            case "http+csv":
                return replaceScheme(uri.toUri(), "http");

            case "csv+https":
            case "https+csv":
                return replaceScheme(uri.toUri(), "https");

            case "csv+file":
            case "file+csv":
            case "csv":
                return replaceScheme(uri.toUri(), "file");

            default:
                throw new IllegalArgumentException(String.format("Unexpected scheme '%s'", uri.scheme()));
        }
    }

    private static String replaceScheme(URI other, String newScheme) {
        try {
            return new URI(newScheme, other.getUserInfo(), other.getHost(), other.getPort(), other.getPath(),
                    other.getQuery(), other.getFragment()).toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}

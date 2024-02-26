/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.Table;
import io.deephaven.json.jackson.JacksonTable;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;

@Immutable
@BuildableStyle
public abstract class JsonTableOptions {

    public static Builder builder() {
        return ImmutableJsonTableOptions.builder();
    }

    public abstract ValueOptions options();

    public abstract Source source();

    @Default
    public boolean isDelimited() {
        // TODO: newline delimited pretty common?
        return false;
    }

    public final Table execute() {
        // This is the only reference from io.deephaven.json into io.deephaven.json.jackson. If we want to break out
        // io.deephaven.json.jackson into a separate project, we'd probably want a ServiceLoader pattern here to choose
        // a default implementation.
        return JacksonTable.execute(this);
    }

    public interface Builder {
        Builder options(ValueOptions options);

        Builder source(Source source);

        default Builder source(File file) {
            return source(Source.of(file));
        }

        default Builder source(Path path) {
            return source(Source.of(path));
        }

        default Builder source(InputStream inputStream) {
            return source(Source.of(inputStream));
        }

        JsonTableOptions build();
    }
}

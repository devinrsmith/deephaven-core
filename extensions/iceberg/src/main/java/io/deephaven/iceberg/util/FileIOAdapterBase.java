//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.io.FileIO;

import java.util.Objects;
import java.util.Optional;

public abstract class FileIOAdapterBase<F extends FileIO, S> implements FileIOAdapter {

    private final Class<F> clazz;

    public FileIOAdapterBase(Class<F> clazz) {
        this.clazz = Objects.requireNonNull(clazz);
    }

    @Override
    public final Optional<SeekableChannelsProvider> provider(FileIO io, Object object) {
        // noinspection unchecked
        return clazz.isInstance(io)
                ? Optional.of(impl(clazz.cast(io), (S) object))
                : Optional.empty();
    }

    protected abstract SeekableChannelsProvider impl(F io, S specialInstructions);
}

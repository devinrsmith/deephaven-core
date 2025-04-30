//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.io.FileIO;

import java.util.Optional;
import java.util.ServiceLoader;

public interface FileIOAdapter {

    static SeekableChannelsProvider fromServiceLoader(FileIO io, Object specialInstructions) {
        for (FileIOAdapter adapter : ServiceLoader.load(FileIOAdapter.class)) {
            final SeekableChannelsProvider provider = adapter.provider(io, specialInstructions).orElse(null);
            if (provider != null) {
                return provider;
            }
        }
        throw new UnsupportedOperationException("No provider found for FileIO " + io.getClass());
    }

    Optional<SeekableChannelsProvider> provider(FileIO io, Object specialInstructions);
}

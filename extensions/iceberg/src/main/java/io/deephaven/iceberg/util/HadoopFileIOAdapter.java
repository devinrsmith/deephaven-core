//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import com.google.auto.service.AutoService;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.apache.iceberg.hadoop.HadoopFileIO;

@AutoService(FileIOAdapter.class)
public final class HadoopFileIOAdapter extends FileIOAdapterBase<HadoopFileIO, Object> {

    public HadoopFileIOAdapter() {
        super(HadoopFileIO.class);
    }

    @Override
    protected SeekableChannelsProvider impl(HadoopFileIO io, Object specialInstructions) {
        return SeekableChannelsProviderLoader.getInstance().load("file", null);
    }
}

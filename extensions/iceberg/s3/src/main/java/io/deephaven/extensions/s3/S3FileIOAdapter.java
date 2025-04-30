//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import com.google.auto.service.AutoService;
import io.deephaven.iceberg.util.FileIOAdapter;
import io.deephaven.iceberg.util.FileIOAdapterBase;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.aws.s3.S3FileIO;

@AutoService(FileIOAdapter.class)
public final class S3FileIOAdapter extends FileIOAdapterBase<S3FileIO, S3Instructions> {

    public S3FileIOAdapter() {
        super(S3FileIO.class);
    }

    @Override
    protected SeekableChannelsProvider impl(S3FileIO io, S3Instructions s3Instructions) {
        // TODO: universal
        return new S3SeekableChannelProvider(io.asyncClient(),
                s3Instructions == null ? S3Instructions.DEFAULT : s3Instructions);
    }
}

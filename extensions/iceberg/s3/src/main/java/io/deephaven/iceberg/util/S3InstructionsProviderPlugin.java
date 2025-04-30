//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import com.google.auto.service.AutoService;
import io.deephaven.extensions.s3.DeephavenAwsClientFactory;
import io.deephaven.extensions.s3.S3Instructions;
import io.deephaven.iceberg.internal.DataInstructionsProviderPlugin;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * {@link DataInstructionsProviderPlugin} implementation for producing a {@link S3Instructions}. The produced
 * instructions will be from {@link DeephavenAwsClientFactory#getInstructions(Map)} if present, and otherwise will make
 * a best-effort attempt to create an equivalent instructions based on properties from {@link AwsClientProperties} and
 * {@link S3FileIOProperties}.
 */
@AutoService(DataInstructionsProviderPlugin.class)
@SuppressWarnings("unused")
public final class S3InstructionsProviderPlugin implements DataInstructionsProviderPlugin {

    @Override
    public S3Instructions createInstructions(@NotNull final String uriScheme,
            @NotNull final Map<String, String> properties) {
        final S3Instructions s3Instructions = DeephavenAwsClientFactory.getInstructions(properties).orElse(null);
        if (s3Instructions != null) {
            return s3Instructions;
        }
        // We have no useful properties for creating an S3Instructions object.
        return null;
    }
}

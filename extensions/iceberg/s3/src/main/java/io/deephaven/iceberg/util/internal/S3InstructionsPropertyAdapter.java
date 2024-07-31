//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util.internal;

import com.google.auto.service.AutoService;
import io.deephaven.extensions.s3.Credentials;
import io.deephaven.extensions.s3.S3Instructions;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.net.URI;
import java.util.Map;

@AutoService(io.deephaven.iceberg.util.internal.PropertyAdapter.class)
public final class S3InstructionsPropertyAdapter implements PropertyAdapter {

    public static void adapt(S3Instructions instructions, Map<String, String> propertiesOut) {
        instructions.regionName().ifPresent(region -> propertiesOut.put(AwsClientProperties.CLIENT_REGION, region));
        final Credentials credentials = instructions.credentials();
        if (credentials == Credentials.anonymous()) {
            propertiesOut.put(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
                    AnonymousCredentialsProvider.class.getName());
        } else if (credentials == Credentials.defaultCredentials()) {
            propertiesOut.put(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
                    DefaultCredentialsProvider.class.getName());
        } else {
            // propertiesOut.put(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER,
            // StaticCredentialsProvider.class.getName());
            propertiesOut.put(S3FileIOProperties.ACCESS_KEY_ID, "todo");
            propertiesOut.put(S3FileIOProperties.SECRET_ACCESS_KEY, "todo");
            throw new UnsupportedOperationException("todo");
        }
        instructions.endpointOverride().map(URI::toString)
                .ifPresent(uri -> propertiesOut.put(S3FileIOProperties.ENDPOINT, uri));
    }

    public S3InstructionsPropertyAdapter() {}

    @Override
    public void adapt(Object dataInstructions, Map<String, String> propertiesOut) {
        if (dataInstructions instanceof S3Instructions) {
            adapt((S3Instructions) dataInstructions, propertiesOut);
        }
    }
}

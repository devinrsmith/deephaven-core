package io.deephaven.client.examples;

import io.deephaven.uri.RemoteDeephavenUri;
import picocli.CommandLine.ITypeConverter;

class DeephavenUriConverter implements ITypeConverter<RemoteDeephavenUri> {

    @Override
    public RemoteDeephavenUri convert(String value) {
        final RemoteDeephavenUri uri = RemoteDeephavenUri.of(value);
        if (uri.isLocal()) {
            throw new IllegalArgumentException("Clients aren't able to use local URIs");
        }
        return uri;
    }
}

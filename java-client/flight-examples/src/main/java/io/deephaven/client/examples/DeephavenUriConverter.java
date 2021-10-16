package io.deephaven.client.examples;

import io.deephaven.DeephavenUri;
import picocli.CommandLine.ITypeConverter;

class DeephavenUriConverter implements ITypeConverter<DeephavenUri> {

    @Override
    public DeephavenUri convert(String value) {
        final DeephavenUri uri = DeephavenUri.of(value);
        if (uri.isLocal()) {
            throw new IllegalArgumentException("Clients aren't able to use local URIs");
        }
        return uri;
    }
}

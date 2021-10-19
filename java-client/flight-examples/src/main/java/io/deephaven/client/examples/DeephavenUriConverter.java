package io.deephaven.client.examples;

import io.deephaven.DeephavenUriI;
import picocli.CommandLine.ITypeConverter;

class DeephavenUriConverter implements ITypeConverter<DeephavenUriI> {

    @Override
    public DeephavenUriI convert(String value) {
        final DeephavenUriI uri = DeephavenUriI.from(value);
        if (uri.isLocal()) {
            throw new IllegalArgumentException("Clients aren't able to use local URIs");
        }
        return uri;
    }
}

package io.deephaven.client.examples;

import io.deephaven.uri.RemoteUri;
import picocli.CommandLine.ITypeConverter;

import java.net.URI;

class RemoteUriConverter implements ITypeConverter<RemoteUri> {

    @Override
    public RemoteUri convert(String value) {
        return RemoteUri.of(URI.create(value));
    }
}

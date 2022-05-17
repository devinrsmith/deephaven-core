package io.deephaven.client.examples;

import io.deephaven.server.config.SSLConfig;
import picocli.CommandLine.ITypeConverter;

import java.io.IOException;
import java.nio.file.Paths;

class SSLConverter implements ITypeConverter<SSLConfig> {

    @Override
    public SSLConfig convert(String file) throws IOException {
        return SSLConfig.parseJson(Paths.get(file));
    }
}

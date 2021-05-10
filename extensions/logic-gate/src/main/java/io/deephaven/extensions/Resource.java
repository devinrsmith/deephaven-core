package io.deephaven.extensions;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class Resource {

    public static Resource of(String resourcePath) {
        return ImmutableResource.builder().resourcePath(resourcePath).build();
    }

    public abstract String resourcePath();

    public final byte[] contentBytes() throws IOException {
        InputStream in = getClass().getResourceAsStream(resourcePath());
        if (in == null) {
            throw new IllegalStateException(
                String.format("Unable to find resource '%s'", resourcePath()));
        }
        try (final BufferedInputStream bis = new BufferedInputStream(in)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int b;
            while ((b = bis.read()) != -1) {
                out.write(b);
            }
            return out.toByteArray();
        }
    }

    public final String contentString(Charset charset) throws IOException {
        return new String(contentBytes(), charset);
    }
}

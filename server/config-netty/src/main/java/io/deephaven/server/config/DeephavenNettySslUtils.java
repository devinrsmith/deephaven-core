package io.deephaven.server.config;

import io.netty.handler.ssl.SslContextBuilder;
import nl.altindag.ssl.util.NettySslUtils;

public class DeephavenNettySslUtils {
    public static SslContextBuilder forServer(SSLConfig config) {
        return NettySslUtils.forServer(DeephavenSslUtils.create(config));
    }

    public static SslContextBuilder forClient(SSLConfig config) {
        return NettySslUtils.forClient(DeephavenSslUtils.create(config));
    }
}

package io.deephaven.server.config;

import io.netty.handler.ssl.SslContextBuilder;
import nl.altindag.ssl.util.NettySslUtils;

public class DeephavenNettySslUtils {
    public static SslContextBuilder forServer(SSLConfig config) {
        return NettySslUtils.forServer(DeephavenSslUtils.create(config));
    }
}

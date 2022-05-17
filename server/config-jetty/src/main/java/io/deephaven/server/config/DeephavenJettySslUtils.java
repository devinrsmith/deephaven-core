package io.deephaven.server.config;

import nl.altindag.ssl.util.JettySslUtils;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public class DeephavenJettySslUtils {
    public static SslContextFactory.Server forServer(SSLConfig config) {
        return JettySslUtils.forServer(DeephavenSslUtils.create(config));
    }

    public static SslContextFactory.Client forClient(SSLConfig config) {
        return JettySslUtils.forClient(DeephavenSslUtils.create(config));
    }
}

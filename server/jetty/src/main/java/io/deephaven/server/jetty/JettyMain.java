package io.deephaven.server.jetty;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.jetty.JettyConfig.Builder;
import io.deephaven.server.runner.Main;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class JettyMain extends Main {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
        final Configuration config = init(args, Main.class);

        // defaults to 5 minutes
        int httpSessionExpireMs = config.getIntegerWithDefault("http.session.durationMs", 300000);
        int httpPort = config.getIntegerWithDefault("http.port", 10000);
        int schedulerPoolSize = config.getIntegerWithDefault("scheduler.poolSize", 4);
        int maxInboundMessageSize = config.getIntegerWithDefault("grpc.maxInboundMessageSize", 100 * 1024 * 1024);

        Builder builder = JettyConfig.builder();
        Main.parseSSLConfig(config).ifPresent(builder::ssl);

        DaggerJettyServerComponent
                .builder()
                .withJettyConfig(builder
                        .tokenExpire(Duration.ofMillis(httpSessionExpireMs))
                        .port(httpPort)
                        .schedulerPoolSize(schedulerPoolSize)
                        .maxInboundMessageSize(maxInboundMessageSize)
                        .build())
                .withOut(PrintStreamGlobals.getOut())
                .withErr(PrintStreamGlobals.getErr())
                .build()
                .getServer()
                .run();
    }
}

package io.deephaven.server.netty;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.netty.NettyConfig.Builder;
import io.deephaven.server.runner.Main;
import io.deephaven.ssl.config.PrivateKeyConfig;
import io.deephaven.ssl.config.SSLConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class NettyMain extends Main {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
        final Configuration config = init(args, Main.class);

        // defaults to 5 minutes
        int httpSessionExpireMs = config.getIntegerWithDefault("http.session.durationMs", 300000);
        int httpPort = config.getIntegerWithDefault("http.port", 8080);
        int schedulerPoolSize = config.getIntegerWithDefault("scheduler.poolSize", 4);
        int maxInboundMessageSize = config.getIntegerWithDefault("grpc.maxInboundMessageSize", 100 * 1024 * 1024);

        String sslCa = config.getStringWithDefault("ssl.identity.ca", null);
        String sslKey = config.getStringWithDefault("ssl.identity.key", null);

        Builder builder = NettyConfig.builder();

        if (sslCa != null && sslKey != null) {
            PrivateKeyConfig identity = PrivateKeyConfig.builder().certChainPath(sslCa).privateKeyPath(sslKey).build();
            SSLConfig ssl = SSLConfig.builder().addIdentity(identity).build();
            builder.ssl(ssl);
        }

        DaggerNettyServerComponent
                .builder()
                .withNettyConfig(builder
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

package io.deephaven.server.netty;

import dagger.Module;
import dagger.Provides;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.server.config.DeephavenNettySslUtils;
import io.deephaven.server.config.SSLConfig;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.runner.GrpcServer;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;

import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.util.Set;

@Module
public class NettyServerModule {
    @Provides
    static GrpcServer serverBuilder(
            ServerConfig serverConfig,
            Set<BindableService> services,
            Set<ServerInterceptor> interceptors) {
        final NettyServerBuilder serverBuilder =
                NettyServerBuilder.forAddress(new InetSocketAddress(serverConfig.host(), serverConfig.port()));
        services.forEach(serverBuilder::addService);
        interceptors.forEach(serverBuilder::intercept);
        serverBuilder.maxInboundMessageSize(serverConfig.maxInboundMessageSize());
        if (serverConfig.ssl().isPresent()) {
            final SSLConfig ssl = serverConfig.ssl().get();
            try {
                serverBuilder.sslContext(GrpcSslContexts.configure(DeephavenNettySslUtils.forServer(ssl)).build());
            } catch (SSLException e) {
                throw new UncheckedDeephavenException(e);
            }
        }
        Server server = serverBuilder.directExecutor().build();
        return GrpcServer.of(server);
    }
}

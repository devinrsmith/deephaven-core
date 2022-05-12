package io.deephaven.server.netty;

import dagger.Module;
import dagger.Provides;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.server.config.KeySourceConfig;
import io.deephaven.server.config.KeySourceConfig.Visitor;
import io.deephaven.server.config.KeyStoreConfig;
import io.deephaven.server.config.PrivateKeyConfig;
import io.deephaven.server.config.SSLConfig;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.runner.GrpcServer;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.inject.Named;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Module
public class NettyServerModule {
    @Provides
    static GrpcServer serverBuilder(
            ServerConfig serverConfig,
            Set<BindableService> services,
            Set<ServerInterceptor> interceptors) {
        final NettyServerBuilder serverBuilder = NettyServerBuilder.forAddress(new InetSocketAddress(serverConfig.host(), serverConfig.port()));
        services.forEach(serverBuilder::addService);
        interceptors.forEach(serverBuilder::intercept);
        serverBuilder.maxInboundMessageSize(serverConfig.maxInboundMessageSize());
        if (serverConfig.ssl().isPresent()) {
            try {
                serverBuilder.sslContext(sslContext(serverConfig.ssl().get()));
            } catch (SSLException e) {
                throw new UncheckedDeephavenException(e);
            }
        }
        Server server = serverBuilder.directExecutor().build();
        return GrpcServer.of(server);
    }

    private static SslContext sslContext(SSLConfig sslConfig) throws SSLException {
        final SslContextBuilder builder = sslConfig.key().walk(new Visitor<SslContextBuilder>() {
            @Override
            public SslContextBuilder visit(KeyStoreConfig keyStore) {
                throw new IllegalStateException("Unable to set KeyStoreConfig via ServerBuilder");
            }

            @Override
            public SslContextBuilder visit(PrivateKeyConfig privateKey) {
                return SslContextBuilder.forServer(new File(privateKey.certChainPath()), new File(privateKey.privateKeyPath()));
            }
        });
        final KeySourceConfig trustsource = sslConfig.trust().orElse(null);
        if (trustsource != null) {
            trustsource.walk(new Visitor<Void>() {
                @Override
                public Void visit(KeyStoreConfig keyStore) {
                    throw new IllegalStateException("TODO: Unable to set KeyStoreConfig via ServerBuilder");
                }

                @Override
                public Void visit(PrivateKeyConfig privateKey) {
                    throw new IllegalStateException("TODO");
                }
            });
        }
        return builder.build();
    }
}

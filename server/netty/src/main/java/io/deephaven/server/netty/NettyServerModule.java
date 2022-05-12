package io.deephaven.server.netty;

import dagger.Module;
import dagger.Provides;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.server.config.*;
import io.deephaven.server.config.KeySourceConfig.Visitor;
import io.deephaven.server.runner.GrpcServer;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLException;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.Set;

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
                return GrpcSslContexts.forServer(new File(privateKey.certChainPath()), new File(privateKey.privateKeyPath()));
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

package io.deephaven.server.jetty;

import com.fasterxml.jackson.databind.ObjectMapper;
import dagger.Module;
import dagger.Provides;
import io.deephaven.server.runner.GrpcServer;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import io.grpc.servlet.jakarta.ServletAdapter;
import io.grpc.servlet.jakarta.ServletServerBuilder;

import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;

@Module
public class JettyServerModule {

    @Provides
    static GrpcServer bindServer(JettyBackedGrpcServer jettyBackedGrpcServer) {
        return jettyBackedGrpcServer;
    }

    @Provides
    static ServletAdapter provideGrpcServletAdapter(
            final @Named("grpc.maxInboundMessageSize") int maxMessageSize,
            final Set<BindableService> services,
            final Set<ServerInterceptor> interceptors) {
        final ServletServerBuilder serverBuilder = new ServletServerBuilder();
        services.forEach(serverBuilder::addService);
        interceptors.forEach(serverBuilder::intercept);

        serverBuilder.maxInboundMessageSize(maxMessageSize);

        serverBuilder.directExecutor();
        return serverBuilder.buildServletAdapter();
    }

    @Provides
    static ServerConfig providesServerConfig(@Named("http.port") int port) {
        final ObjectMapper om = new ObjectMapper();
        om.findAndRegisterModules();
        // om.registerModule(new Jdk8Module());
        // hack:
        try {
            return om.readValue(new File("deephaven.json"), ServerConfig.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

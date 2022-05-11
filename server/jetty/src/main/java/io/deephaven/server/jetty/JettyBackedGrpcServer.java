package io.deephaven.server.jetty;

import io.deephaven.server.runner.GrpcServer;
import jakarta.servlet.DispatcherType;
import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http2.parser.RateControl;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ErrorPageErrorHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static org.eclipse.jetty.servlet.ServletContextHandler.SESSIONS;

public class JettyBackedGrpcServer implements GrpcServer {

    private final Server jetty;

    @Inject
    public JettyBackedGrpcServer(
            final @Named("http.port") int port,
            final GrpcFilter filter) {
        jetty = new Server();

        // https://www.eclipse.org/jetty/documentation/jetty-11/programming-guide/index.html#pg-server-http-connector-protocol-http2-tls
        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.addCustomizer(new SecureRequestCustomizer());

//        HttpConnectionFactory http11 = new HttpConnectionFactory(httpConfig);

        // note: java client breaking when using HTTP2CServerConnectionFactory
        HTTP2ServerConnectionFactory h2 = new HTTP2ServerConnectionFactory(httpConfig);
        ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory();
//        alpn.setDefaultProtocol(http11.getProtocol());
        alpn.setDefaultProtocol(h2.getProtocol());

        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
        sslContextFactory.setKeyStorePath("/home/devin/dev/deephaven/deephaven-core/data/felian.fish-moth.ts.net.chain.p12");
        sslContextFactory.setKeyStorePassword("test");

        SslConnectionFactory tls = new SslConnectionFactory(sslContextFactory, alpn.getProtocol());

//        ServerConnector sc = new ServerConnector(jetty, tls, alpn, h2, http11);
        ServerConnector sc = new ServerConnector(jetty, tls, alpn, h2);
        sc.setPort(8443);
        jetty.addConnector(sc);

        final WebAppContext context =
                new WebAppContext(null, "/", null, null, null, new ErrorPageErrorHandler(), SESSIONS);
        try {
            String knownFile = "/ide/index.html";
            URL ide = JettyBackedGrpcServer.class.getResource(knownFile);
            context.setBaseResource(Resource.newResource(ide.toExternalForm().replace("!" + knownFile, "!/")));
        } catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }


        // For the Web UI, cache everything in the static folder
        // https://create-react-app.dev/docs/production-build/#static-file-caching
        context.addFilter(NoCacheFilter.class, "/iriside/*", EnumSet.noneOf(DispatcherType.class));
        context.addFilter(CacheFilter.class, "/iriside/static/*", EnumSet.noneOf(DispatcherType.class));

        // Always add eTags
        context.setInitParameter("org.eclipse.jetty.servlet.Default.etags", "true");
        context.setSecurityHandler(new ConstraintSecurityHandler());

        // Add an extra filter to redirect from / to /ide/
        context.addFilter(HomeFilter.class, "/", EnumSet.noneOf(DispatcherType.class));

        // Direct jetty all use this configuration as the root application
        context.setContextPath("/");

        // Wire up the provided grpc filter
        context.addFilter(new FilterHolder(filter), "/*", EnumSet.noneOf(DispatcherType.class));

        // Set up websocket for grpc-web
//        JakartaWebSocketServletContainerInitializer.configure(context, (servletContext, container) -> {
//            container.addEndpoint(
//                    ServerEndpointConfig.Builder.create(WebSocketServerStream.class, "/{service}/{method}")
//                            .configurator(new ServerEndpointConfig.Configurator() {
//                                @Override
//                                public <T> T getEndpointInstance(Class<T> endpointClass) throws InstantiationException {
//                                    return (T) filter.create(WebSocketServerStream::new);
//                                }
//                            })
//                            .build());
//        });

        jetty.setHandler(context);

    }

    @Override
    public void start() throws IOException {
        try {
            jetty.start();
        } catch (RuntimeException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public void join() throws InterruptedException {
        jetty.join();
    }

    @Override
    public void stopWithTimeout(long timeout, TimeUnit unit) {
        jetty.setStopTimeout(unit.toMillis(timeout));
        Thread shutdownThread = new Thread(() -> {
            try {
                jetty.stop();
            } catch (Exception exception) {
                throw new IllegalStateException("Failure while stopping", exception);
            }
        });
        shutdownThread.start();
    }

    @Override
    public int getPort() {
        return ((ServerConnector) jetty.getConnectors()[0]).getLocalPort();
    }
}

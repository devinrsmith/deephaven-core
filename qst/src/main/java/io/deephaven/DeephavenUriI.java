package io.deephaven;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.OptionalInt;

public interface DeephavenUriI {

    String SCHEME = "dh";

    Path APPLICATION = Paths.get("a");

    Path FIELD = Paths.get("f");

    Path QUERY_SCOPE = Paths.get("s");

    Path PROXY = Paths.get("h");

    static DeephavenUriI from(String uri) {
        return from(URI.create(uri));
    }

    static DeephavenUriI from(URI uri) {
        if (!SCHEME.equals(uri.getScheme())) {
            throw new IllegalArgumentException(String.format("Invalid Deephaven URI scheme '%s'", uri.getScheme()));
        }
        if (uri.isOpaque()) {
            throw new IllegalArgumentException("Deephaven URIs are not opaque");
        }
        if (uri.getPath() == null || uri.getPath().charAt(0) != '/') {
            throw new IllegalArgumentException("Deephavhen URI path must be absolute");
        }
        if (uri.getUserInfo() != null) {
            throw new IllegalArgumentException("Deephaven URI does not support user info at this time");
        }
        if (uri.getQuery() != null) {
            throw new IllegalArgumentException("Deephaven URI does not support query params at this time");
        }
        if (uri.getFragment() != null) {
            throw new IllegalArgumentException("Deephaven URI does not support fragments at this time");
        }
        // Strip absolute path '/' from URI
        final Path path = Paths.get(uri.getPath().substring(1));
        if (uri.getHost() == null) {
            return from(path);
        }
        if (uri.getPort() == -1) {
            return from(uri.getHost(), path);
        }
        return from(uri.getHost(), uri.getPort(), path);
    }

    static DeephavenUriI from(Path path) {
        if (DeephavenUriQueryScope.isMatch(path)) {
            return DeephavenUriQueryScope.builder().parse(path).build();
        }
        if (DeephavenUriApplicationField.isMatch(path)) {
            return DeephavenUriApplicationField.builder().parse(path).build();
        }
        if (DeephavenUriField.isMatch(path)) {
            return DeephavenUriField.builder().parse(path).build();
        }
        if (DeephavenUriProxy.isMatch(path)) {
            return DeephavenUriProxy.builder().parse(path).build();
        }
        throw new IllegalArgumentException();
    }

    static DeephavenUriI from(String host, Path path) {
        if (DeephavenUriQueryScope.isMatch(path)) {
            return DeephavenUriQueryScope.builder().host(host).parse(path).build();
        }
        if (DeephavenUriApplicationField.isMatch(path)) {
            return DeephavenUriApplicationField.builder().host(host).parse(path).build();
        }
        if (DeephavenUriField.isMatch(path)) {
            return DeephavenUriField.builder().host(host).parse(path).build();
        }
        if (DeephavenUriProxy.isMatch(path)) {
            return DeephavenUriProxy.builder().host(host).parse(path).build();
        }
        throw new IllegalArgumentException();
    }

    static DeephavenUriI from(String host, int port, Path path) {
        if (DeephavenUriQueryScope.isMatch(path)) {
            return DeephavenUriQueryScope.builder().host(host).port(port).parse(path).build();
        }
        if (DeephavenUriApplicationField.isMatch(path)) {
            return DeephavenUriApplicationField.builder().host(host).port(port).parse(path).build();
        }
        if (DeephavenUriField.isMatch(path)) {
            return DeephavenUriField.builder().host(host).port(port).parse(path).build();
        }
        if (DeephavenUriProxy.isMatch(path)) {
            return DeephavenUriProxy.builder().host(host).port(port).parse(path).build();
        }
        throw new IllegalArgumentException();
    }

    Optional<String> host();

    OptionalInt port();

    URI toUri();

    Path path();

    <V extends Visitor> V walk(V visitor);

    DeephavenUriI proxyVia(String host);

    DeephavenUriI proxyVia(String host, int port);

    interface Visitor {
        void visit(DeephavenUriField field);

        void visit(DeephavenUriApplicationField applicationField);

        void visit(DeephavenUriQueryScope queryScope);

        void visit(DeephavenUriProxy proxy);
    }
}
